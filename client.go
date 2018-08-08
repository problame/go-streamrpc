package streamrpc

import (
	"bytes"
	"io"
	"net"
	"errors"
	"github.com/problame/go-streamrpc/internal/pdu"
	"context"
    "fmt"
    "sync"
)

type ClientConfig struct {
    // Config for established connections
    ConnConfig *ConnConfig
}

func (cf *ClientConfig) Validate() error {
	if cf == nil {
		return errors.New("ClientConfig must not be nil")
	}
	if err := cf.ConnConfig.Validate(); err != nil {
		return fmt.Errorf("ClientConfig invalid: %s", err)
	}
	return nil
}

type Client struct {
	cf	*ClientConfig
	cm	connMan
}

type connMan struct {
	cf  	*ClientConfig
	mtx 	sync.Mutex
	stopped	bool
	c		*Conn
	cn		Connecter
}

type Connecter interface {
	Connect(ctx context.Context) (net.Conn, error)
}

func NewClient(connecter Connecter, config *ClientConfig) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	client := &Client{
		cf: config,
		cm: connMan{
			cf: config,
			cn: connecter,
		},
	}
	return client, nil

}

func NewClientOnConn(conn net.Conn, config *ClientConfig) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	c, err := newConn(conn, config.ConnConfig)
	if err != nil {
		return nil, err
	}
	return &Client{
		cf: config,
		cm: connMan{
			cf: config,
			c: c,
		},
	}, nil
}

var (
	ErrorRPCClientClosed = errors.New("use of closed RPC client")
)

// may return nil, nil
func (m *connMan) getConn(ctx context.Context, reconnect bool) (*Conn, error) {
    m.mtx.Lock()
    defer m.mtx.Unlock()

    if !m.stopped && reconnect && (m.c == nil || m.c.Closed()) {
        if m.cn == nil {
            // for NewClientOnConn
            return nil, errors.New("cannot reconnect without connector")
        }

		log := logger(ctx)
		log.Printf("connecting to server")

		netConn, err := m.cn.Connect(ctx)
		if err != nil {
			return nil, err
		}

		m.c, err = newConn(netConn, m.cf.ConnConfig)
		if err != nil {
			if err := netConn.Close(); err != nil {
				log.Printf("error closing connection after failed protocol handshake: %s", err)
		    	}
			return nil, err
		}

		return m.c, err
	}
	if m.stopped {
		if m.c != nil {
			m.c.Close()
		}
		return nil, ErrorRPCClientClosed
	}
	return m.c, nil
}

func (m *connMan) stop() {
	m.stopped = true
}

type RemoteEndpointError struct {
	msg string
}

func (e *RemoteEndpointError) Error() string {
	return e.msg
}

var (
	ErrorConcurrentRequestReply = errors.New("concurrent use of RPC connection")
	ErrorRequestReplyWithOpenStream = errors.New("cannot use RPC connection until result stream from a previous call is closed")
)

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
func (c *Client) RequestReply(ctx context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (*bytes.Buffer, *Stream, error) {

	conn, err := c.cm.getConn(ctx, true)
	if err != nil {
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
		err := conn.send(&hdr, reqStructured, reqStream)
		rchan <- result{nil, err, err != nil && err != errorConcurrentSend} // FIXME: always close is correct right now because c.c.send calls writeStream which hides error returned by reqStream. However, maybe we should in fact not hide that?
	}()
	go func() {
		r := conn.recv()
		close := false
		var err error = nil
		if r.err != nil {
			err = r.err
			close = true
			if r.err == errorRecvWithOpenStream || r.err == errorConcurrentRecv {
				close = false
			}
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
				conn.Close()
				return nil, nil, ctx.Err()
			case res = <- rchan:
				if res.close {
					conn.Close()
				}
				if res.err != nil {
					if res.err == errorConcurrentRecv || res.err == errorConcurrentSend {
						return nil, nil, ErrorConcurrentRequestReply
					}
					if res.err == errorRecvWithOpenStream {
						return nil, nil, ErrorRequestReplyWithOpenStream
					}
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
	c.cm.stop()
	if conn, _ := c.cm.getConn(context.Background(), false); conn != nil {
		// ignore potential concurrent use errors
		// FIXME: specify send timeout / do we even need this?
		conn.send(&pdu.Header{Close: true}, nil, nil)
		conn.Close()
	}
}
