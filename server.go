package streamrpc

import (
	"io"
	"bytes"
	"github.com/problame/go-streamrpc/internal/pdu"
	"net"
	"context"
	"fmt"
)

// HandlerFunc is called from ServeConn's server loop to handle Client requests.
//
// If reqStream != nil, the handler must Close() reqStream before returning.
// Otherwise, ServeConn will close the connection and will return
// a *HandlerInputStreamNotClosedError which wraps the original handler error.
//
// If reqStream was closed but not consumed, ServeConn also closes the connection because it is left in an unknown
// state (who knows how many bytes of the reqStream are already on the way from sender to receiver...).
// This condition is signaled by ServeConn by returning a *HandlerInputStreamNotFullyConsumed, which
// wraps the original handler error.
//
// If the handler returns resStream != nil, ServeConn will send resStream to the client where it is
// represented as a *Stream.
// If resStream.Read returns an error e, the client will receive the string returned by e.Error encapsulated in a
// *StreamError instance when Read()ing from the *Stream.
// To clean up resources held by resStream, ServeConn checks if resStream.(io.Closer), and if so, calls
// resStream.Close() regardless of whether resStream.Read returned an error other than io.EOF.
type HandlerFunc func(ctx context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error)

type HandlerInputStreamNotClosedError struct {
	HandlerError error
}

func (e *HandlerInputStreamNotClosedError) Error() string {
	if e.HandlerError != nil {
		return fmt.Sprintf("implementation error: streamrpc handler did not close input stream (returned error (%T) %q",
			e.HandlerError, e.HandlerError)
	}
	return fmt.Sprintf("implementation error: streamrpc handler did not close input stream (no error returned)")
}

type HandlerInputStreamNotFullyConsumed struct {
	HandlerError error
}

func (e *HandlerInputStreamNotFullyConsumed) Error() string {
	if e.HandlerError == nil {
		return "handler did not consume the input stream but did not return an error"
	}
	return fmt.Sprintf("handler error with unconsumed input stream: %s", e.HandlerError.Error())
}

// ServeConn consumes the netConn, i.e., it responds to requests it receives over netConn by calling handler until an
// error on netConn.Read or netConn.Write, or a protocol error occurs.
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
		if err := netConn.Close(); err != nil {
			log.Errorf("error closing connection after failed protocol handshake: %s", err)
		}
		return err
	}
	defer func() {
		log.Infof("closing connection")
		if err :=  conn.Close(); err != nil {
			log.Errorf("error closing connection: %s", err)
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

		log.Infof("incoming request endpoint=%q", r.header.Endpoint)
		if r.stream != nil {
			r.stream.closeConnOnCloseAndUnconsumed = false // will handle that with abortAfterResponse
		}
		resStructured, resStream, err := handler(ctx, r.header.Endpoint, r.structured, r.stream)
		var abortAfterResponse error = nil
		if r.stream != nil {
			closed, consumed := r.stream.State()
			if !closed {
				abortAfterResponse = &HandlerInputStreamNotClosedError{HandlerError: err}
			} else if !consumed {
				abortAfterResponse = &HandlerInputStreamNotFullyConsumed{HandlerError: err}
				log.Infof("handler did not fully consume stream")
			}
		}

		log.Infof("start componsing response: %T %T %T", resStructured, resStream, err)

		hdr := pdu.Header{
			Close: abortAfterResponse != nil,
		}
		if abortAfterResponse != nil {
			log.Infof("will close connection because of handler's input stream handling (see above)")
			hdr.EndpointError = abortAfterResponse.Error()
		} else if err != nil {
			hdr.EndpointError = err.Error()
		}
		log.Infof("start sending resonse")
		sendErr := conn.send(&hdr, resStructured, resStream)
		if resStream != nil {
			if closer, ok := resStream.(io.Closer); ok {
				if err := closer.Close(); err != nil {
					log.Errorf("error closing stream returned from handler: %s", err)
				}
			}
		}
		if sendErr != nil {
			log.Errorf("error sending repsonse: %s", sendErr)
			return sendErr
		}
		log.Infof("finished sending repsonse")

		if abortAfterResponse != nil {
			return abortAfterResponse // defer will close connection
		}

	}

}


