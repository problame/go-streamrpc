package streamrpc

import (
	"bytes"
	"io"
	"net"
	"github.com/pkg/errors"
	"github.com/problame/go-streamrpc/internal/pdu"
	"context"
)

type Client struct {
	cn Connecter
	cf *ConnConfig
	c *Conn
}

type Connecter interface {
	Connect() (net.Conn, error)
}

func NewClient(connecter Connecter, config *ConnConfig) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Client{
		cn: connecter,
		cf: config,
	}, nil
}

func NewClientOnConn(rwc io.ReadWriteCloser, config *ConnConfig) (*Client, error) {
	c, err := newConn(rwc, config)
	if err != nil {
		return nil, err
	}
	return &Client{
		cf: config,
		c: c,
	}, nil
}

func (c *Client) reconn() error {
	if !c.c.Valid() {
		if c.cn == nil {
			// for NewClientOnConn
			return errors.New("cannot reconnect without connector")
		}
		netConn, err := c.cn.Connect()
		if err != nil {
			return err
		}
		c.c, err = newConn(netConn, c.cf)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) closeConn(ctx context.Context) {
	if err := c.c.Close(); err != nil {
		logger(ctx).Printf("error closing connection: %s", err)
	}
}

type RemoteEndpointError struct {
	msg string
}

func (e *RemoteEndpointError) Error() string {
	return e.msg
}

// RequestReply sends a request to a remote HandlerFunc and reads its response.
//
// If the endpoint handler returned an error, that error is returned as a *RemoteEndpointError.
// Other returned errors are likely protocol or network errors.
//
// If the endpoint handler returns a *Stream, that stream must be Close()d by the caller.
// Otherwise, RequestReply will block indefinitely.
//
// If reqStream != nil, reqStream will be read until an error e occurs with io.Copy semantics:
// if e == io.EOF, RequestReply does not consider it an error and assumes all data in the stream has been sent.
// If e != io.EOF, the endpoint handler will receive the bytes already copied followed by a *StreamError.
// However, for neither case does RequestReply return an error (FIXME) to the caller.
func (c *Client) RequestReply(ctx context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (*bytes.Buffer, *Stream, error) {

	if err := c.reconn(); err != nil {
		return nil, nil, err
	}

	type result struct {
		r *recvResult // if err == nil, this must be != nil
		err error
		close bool
	}
	rchan := make(chan result, 2) // size 2 to avoid leaking goroutines while also not draining the channel (will be GCd)
	go func() {
		hdr := pdu.Header{Endpoint: endpoint}
		err := c.c.send(&hdr, reqStructured, reqStream)
		rchan <- result{nil, err, err != nil} // FIXME: always close is correct right now because c.c.send calls writeStream which hides error returned by reqStream. However, maybe we should in fact not hide that?
	}()
	go func() {
		r := c.c.recv()
		close := false
		var err error = nil
		if r.err != nil {
			err = r.err
			close = true
		} else if r.header.Close && !(r.header.EndpointError != "") {
			err = errors.New("protocol error: Close=true implies EndpointError!=\"\"")
			close = true
		} else if r.header.EndpointError != "" {
			err = &RemoteEndpointError{msg: r.header.EndpointError}
			close = r.header.Close
		}
		rchan <- result{r, err, close}
	}()

	var res result
	out:
	for {
		select {
			case <-ctx.Done():
				c.closeConn(ctx)
				return nil, nil, ctx.Err()
			case res = <- rchan:
				if res.close {
					c.closeConn(ctx)
				}
				if res.err != nil {
					return nil, nil, res.err
				}
				if res.r != nil {
					break out
				}
		}
	}
	return res.r.structured, res.r.stream, nil
}

func (c *Client) Close() {
	c.c.send(&pdu.Header{Close: true}, nil, nil)
	c.closeConn(context.Background())
}