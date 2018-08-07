package streamrpc

import (
	"io"
	"bytes"
	"github.com/problame/go-streamrpc/internal/pdu"
	"net"
	"context"
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
type HandlerFunc func(ctx context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error)

// ServeConn consumes the rwc, i.e., it responds to requests it receives over rwc by calling handler until an
// error on rwc.Read or rwc.Write, or a protocol error occurs.
// If the error is io.EOF, nil is returned. Otherwise, the returned error will be != nil.
//
// Note that errors returned by the handler do not cause this function to return.
// See HandlerFunc for a description of the expected behavior of handler.
//
// The ctx is passed through to each invocation of handler.
func ServeConn(ctx context.Context, netConn net.Conn, config *ConnConfig, handler HandlerFunc) error {
	log := logger(ctx)

	conn, err := newConn(netConn , config)
	if err != nil {
		return err
	}
	defer func() {
		if err :=  conn.Close(); err != nil {
			log.Printf("error closing connection: %s", err)
		}
	}()

	for {

		r := conn.recv()
		if r.err != nil {
			if r.err == io.EOF {
				// it's OK for the client to just hang up here, no need for Close=1 in header
				return nil
			}
			return r.err
		}

		resStructured, resStream, err := handler(ctx, r.header.Endpoint, r.structured, r.stream)
		if err != nil {
			hdr := pdu.Header{
				EndpointError: err.Error(),
				Close: r.stream != nil, // handler might not have consumed r.stream yet, so the connection is in unknown state
			}
			if err := conn.send(&hdr, nil, nil); err != nil {
				log.Printf("error sending handler-error response: %s", err)
				return err
			}
			if hdr.Close {
				log.Printf("closing connection after handler error on request with a stream")
				return nil // defer will Close it
			}
			continue
		}

		hdr := pdu.Header{}
		err = conn.send(&hdr, resStructured, resStream)
		if closer, ok := resStream.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				log.Printf("error closing stream returned from handler")
			}
		}
		if err != nil {
			return err
		}

	}

}


