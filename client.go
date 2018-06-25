package streamrpc

import (
	"bytes"
	"io"
	"net"
	"github.com/pkg/errors"
	"github.com/problame/go-streamrpc/internal/pdu"
	"fmt"
)

type Client struct {
	cn Connecter
	cf *ConnConfig
	c *Conn
}

type Connecter interface {
	Connect() (net.Conn, error)
}

func NewClient(connecter Connecter, config *ConnConfig) *Client {
	if err := config.Validate(); err != nil {
		panic(err)
	}
	return &Client{
		cn: connecter,
		cf: config,
	}
}

func NewClientOnConn(rwc io.ReadWriteCloser, config *ConnConfig) *Client {
	if err := config.Validate(); err != nil {
		panic(err)
	}
	return &Client{
		cf: config,
		c: newConn(rwc, config),
	}
}

func (c *Client) reconn() error {
	if c.c == nil {
		if c.cn == nil {
			// for NewClientOnConn
			return errors.New("cannot reconnect without connector")
		}
		netConn, err := c.cn.Connect()
		if err != nil {
			return err
		}
		c.c = newConn(netConn, c.cf)
	}
	return nil
}

func (c *Client) closeConn() {
	if err := c.c.Close(); err != nil {
		panic(err) // FIXME revise this when we have logging
	}
	c.c = nil
}

type RemoteEndpointError struct {
	msg string
}

func (e *RemoteEndpointError) Error() string {
	return e.msg
}

func (c *Client) RequestReply(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (resStructured *bytes.Buffer, resStream *Stream, err error) {

	if err := c.reconn(); err != nil {
		return nil, nil, err
	}

	hdr := pdu.Header{Endpoint: endpoint}
	err = c.c.send(&hdr, reqStructured, reqStream)
	if err != nil {
		c.closeConn()
		return nil, nil, err
	}
	rhdr, resStructured, resStream, err := c.c.recv()
	if err != nil {
		c.closeConn()
		return nil, nil, err
	}
	if rhdr.EndpointError != "" {
		return nil, nil, &RemoteEndpointError{msg: rhdr.EndpointError}
	}
	return resStructured, resStream, nil
}

func (c *Client) Close() error {
	// FIXME
	return fmt.Errorf("close not implemented")
}