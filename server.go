package streamrpc

import (
	"io"
	"bytes"
	"github.com/problame/go-streamrpc/internal/pdu"
)

type HandlerFunc func(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (resStructured *bytes.Buffer, resStream io.Reader, err error)

func ServeConn(rwc io.ReadWriteCloser, config *ConnConfig, handler HandlerFunc) error {

	conn := newConn(rwc, config)
	defer conn.Close() // FIXME log error

	for {

		header, reqStructured, reqStream, err := conn.recv()
		if err != nil {
			return err
		}

		resStructured, resStream, err := handler(header.Endpoint, reqStructured, reqStream)
		if err != nil {
			hdr := pdu.Header{
				EndpointError: err.Error(),
			}
			if err := conn.send(&hdr, nil, nil); err != nil {
				return err
			} else {
				continue
			}
		}

		hdr := pdu.Header{}
		err = conn.send(&hdr, resStructured, resStream)
		if closer, ok := resStream.(io.Closer); ok {
			closer.Close() // FIXME log error
		}
		if err != nil {
			return err
		}

	}

}


