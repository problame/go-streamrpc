package streamrpc

import (
	"io"
	"bytes"
	"github.com/problame/go-streamrpc/internal/pdu"
)

// The handler MUST consume reqStream fully (e.g. until an io.EOF occurs) OR it MUST return an error.
// In the latter case, the response sent to the client will indicate that the connection needs to be re-established.
//
// If the handler returns resStream != nil, ServeConn will send resStream to the client where it is
// represented as a *Stream.
// If resStream.Read returns an error e, the client will receive the string returned by e.Error encapsulated in a
// *StreamError instance when Read()ing from the *Stream.
// To clean up resources held by resStream, ServeConn checks if resStream.(io.Closer), and if so, calls
// resStream.Close() regardless of whether resStream.Read returned an error other than io.EOF.
type HandlerFunc func(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (resStructured *bytes.Buffer, resStream io.Reader, err error)

// ServeConn responds to requests it receives over rwc by calling handler.
// See HandlerFunc for a description of the expected behavior of handler.
//
// ServeConn returns with an error if rwc returns an error on Read or Write operations.
// It does not return errors returned by handler - these are sent to the client.
func ServeConn(rwc io.ReadWriteCloser, config *ConnConfig, handler HandlerFunc) error {

	conn := newConn(rwc, config)
	defer conn.Close() // FIXME log error

	for {

		r := conn.recv()
		if r.err != nil {
			return r.err
		}

		resStructured, resStream, err := handler(r.header.Endpoint, r.structured, r.stream)
		if err != nil {
			hdr := pdu.Header{
				EndpointError: err.Error(),
				Close: r.stream != nil, // handler might not have consumed r.stream yet, so the connection is in unknown state
			}
			if err := conn.send(&hdr, nil, nil); err != nil {
				// FIXME log error
				return err
			}
			if hdr.Close {
				// FIXME: flush conn?
				return nil // defer will Close it
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


