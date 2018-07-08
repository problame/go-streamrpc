package streamrpc

import (
	"testing"
	"net"
	"bytes"
	"io"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"time"
	"sync/atomic"
	"runtime"
	"io/ioutil"
	"context"
)

func testClientServerMockConnsServeResult(clientConn, serverConn io.ReadWriteCloser, serveResult chan error, handler HandlerFunc) (client *Client) {
	connConfig := &ConnConfig{
		RxStreamMaxChunkSize: 4 * 1024 * 1024,
		RxHeaderMaxLen:       1024,
		RxStructuredMaxLen:   64 * 1024,
		TxChunkSize:          4,
	}
	clientConfig := &ClientConfig{
		MaxConnectAttempts: 1,
		ReconnectBackoffFactor: 1,
		ReconnectBackoffBase: 10*time.Millisecond,
		ConnConfig: connConfig,
	}

	client, err := NewClientOnConn(clientConn, clientConfig)
	if err != nil {
		panic(err)
	}
	if serveResult == nil {
		serveResult = make(chan error, 1) // just forget it
	}
	go func() {
		serveResult <- ServeConn(serverConn, connConfig, handler)
	}()
	return client
}

func testClientServerMockConns(clientConn, serverConn io.ReadWriteCloser, handler HandlerFunc) (client *Client) {
	return testClientServerMockConnsServeResult(clientConn, serverConn, nil, handler)
}

func testClientServer(handler HandlerFunc) (client *Client) {
	clientConn, serverConn := net.Pipe()
	return testClientServerMockConns(clientConn, serverConn, handler)
}

func TestBehaviorHandlerError(t *testing.T) {

	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return nil, nil, errors.New("test error")
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("foo"), nil)
	assert.Nil(t, stru)
	assert.Nil(t, stre)
	reperr, ok := err.(*RemoteEndpointError)
	assert.True(t, ok)
	assert.EqualError(t, reperr, "test error")

}

func readerToString(r io.Reader) string {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		panic(err)
	}
	return buf.String()
}

func TestBehaviorRequestStreamReply(t *testing.T) {

	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		assert.Equal(t, "foobar", endpoint)
		assert.Equal(t, "question", reqStructured.String())
		assert.Nil(t, reqStream)
		return bytes.NewBufferString("structured"), sReadCloser("stream"), nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)

	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))

}

func TestBehaviorStreamRequestReply(t *testing.T) {
	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		assert.Equal(t, "foobar", endpoint)
		assert.Equal(t, "question", reqStructured.String())
		assert.Equal(t, "stream", readerToString(reqStream))
		return bytes.NewBufferString("structured"), nil, nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), sReadCloser("stream"))
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Nil(t, stre)
}

func TestBehaviorMultipleRequestsOnSameConnection(t *testing.T) {

	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		assert.Equal(t, "foobar", endpoint)
		assert.Equal(t, "question", reqStructured.String())
		assert.Nil(t, reqStream)
		return bytes.NewBufferString("structured"), sReadCloser("stream"), nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))

	stru, stre, err = client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))
}

func TestBehaviorOpenStreamBlocksNextRequest(t *testing.T) {

	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return bytes.NewBufferString("structured"), sReadCloser("stream"), nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())

	var secondRequestDone int32
	go func(){
		atomic.StoreInt32(&secondRequestDone, 1)
		stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("q2"), nil)
		atomic.StoreInt32(&secondRequestDone, 2)
		assert.NoError(t, err)
		assert.Equal(t, "structured", stru.String())
		assert.Equal(t, "stream", readerToString(stre))
	}()

	for atomic.LoadInt32(&secondRequestDone) != 1 {
		runtime.Gosched()
	}
	time.Sleep(100*time.Millisecond)
	assert.False(t, atomic.LoadInt32(&secondRequestDone) == 2,
		"new request should not be allowed to start before previous stream has been fully read")

	_, err = io.Copy(ioutil.Discard, stre)
	assert.NoError(t, err)

	time.Sleep(100*time.Millisecond)
	assert.True(t, atomic.LoadInt32(&secondRequestDone) == 2)


}

type readWriteCloseRecorder struct {
	io.ReadWriteCloser
	closeCount int
}

func (r *readWriteCloseRecorder) Close() error {
	r.closeCount++
	return r.ReadWriteCloser.Close()
}

func TestBehaviorClientClosingUnconsumedStreamClosesConnection(t *testing.T) {

	clientConnP, serverConn := net.Pipe()
	clientConn := &readWriteCloseRecorder{clientConnP, 0}

	client := testClientServerMockConns(clientConn, serverConn, func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return bytes.NewBufferString("structured"), sReadCloser("stream"), nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("q"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())

	err = stre.Close()
	assert.NoError(t, err)

	assert.Equal(t, 1, clientConn.closeCount)
}

func TestBehaviorClientClosesConnectionIfHandlerDoesNotCloseReqStream(t *testing.T) {

	clientConnP, serverConn := net.Pipe()
	clientConn := &readWriteCloseRecorder{clientConnP, 0}
	serveRes := make(chan error, 1)

	client := testClientServerMockConnsServeResult(clientConn, serverConn, serveRes, func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return nil, nil, errors.New("testerror")
	})

	// send reqStream to server, but the handler doesn't consume it but returns an error instead
	// -> the server will send the client a pdu.Header.Close = true
	// ->-> the client will close the connecting

	stru, stre, err := client.RequestReply(context.Background(), "foo", bytes.NewBufferString("q"), sReadCloser("this is a stream that is never read"))
	assert.Nil(t, stru)
	assert.Nil(t, stre)
	assert.EqualError(t, err, "testerror")
	assert.Equal(t, 1, clientConn.closeCount)
	assert.Nil(t, <-serveRes) // ServeConn should return, but from a protocol point of view, things went fine
}

type mockReadCloser struct {
	buf *bytes.Buffer
	readCount int
	closeCount int
}

func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	n, err = m.buf.Read(p)
	m.readCount++
	return
}

func (m *mockReadCloser) Close() error {
	m.closeCount++
	return nil
}

func TestBehaviorServerClosesResStreamIfCloser(t *testing.T) {

	clientConn, serverConn := net.Pipe()
	serverRes := make(chan error, 1)

	mockStream := &mockReadCloser{bytes.NewBufferString("stream"), 0, 0}
	client := testClientServerMockConnsServeResult(clientConn, serverConn, serverRes, func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return bytes.NewBufferString("structured"), mockStream, nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))

	// make server exit
	serverConn.Close()
	assert.Equal(t, io.ErrClosedPipe, <-serverRes)
	assert.Equal(t, 1, mockStream.closeCount)
}

func TestBehaviorClientContextCancel(t *testing.T) {

	clientConnP, serverConn := net.Pipe()
	clientConn := &readWriteCloseRecorder{clientConnP, 0}
	serverErr := make(chan error, 1)

	serverReceivedReq, serverRespond := make(chan struct{}), make(chan struct{})
	client := testClientServerMockConnsServeResult(clientConn, serverConn, serverErr, func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		serverReceivedReq <- struct{}{}
		<-serverRespond
		return bytes.NewBufferString("foo"), nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		stru, stre, err := client.RequestReply(ctx, "foo", bytes.NewBufferString("req"), nil)
		assert.Nil(t, stru)
		assert.Nil(t, stre)
		assert.Equal(t, err, context.Canceled)
		serverRespond <- struct{}{}
	}()

	<- serverReceivedReq
	cancel()

	time.Sleep(100*time.Millisecond)

	assert.Equal(t, 1, clientConn.closeCount)
	assert.Error(t, <-serverErr)

}
