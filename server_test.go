package streamrpc

import (
	"testing"
	"net"
	"bytes"
	"io"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"context"
    "fmt"
    "runtime"
	"time"
)

type testingLogger struct {
	t *testing.T
}

var _ Logger = testingLogger{}

func (l testingLogger) Infof(fmt string, args... interface{}) {
	l.t.Logf(fmt, args...)
}

func (l testingLogger) Errorf(fmt string, args... interface{}) {
	l.t.Logf(fmt, args...)
}

// this is terrible, but we can't use net.Pipe because it does not linger after Close
// which is what the tests expect
func newTestPipe() (net.Conn, net.Conn) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()

	a, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		panic(err)
	}
	b, err := l.Accept()
	if err != nil {
		panic(err)
	}
	banner := [4]byte{0xba,0xdf,0xf0,0x0d}
	bannerCopy := [4]byte{}
	copy(bannerCopy[:], banner[:])
	if n, err := a.Write(bannerCopy[:]); n != len(bannerCopy) || err != nil {
		panic(fmt.Sprintf("%v %v", n, err))
	}
	if n, err := b.Read(bannerCopy[:]); n != len(bannerCopy) || err != nil {
		panic(fmt.Sprintf("%v %v", n, err))
	}
	if !bytes.Equal(banner[:], bannerCopy[:]) {
		panic("connection hijacked")
	}
	return a, b
}
func testClientServerMockConnsServeResult(t *testing.T, clientConn, serverConn net.Conn, serveResult chan error, handler HandlerFunc) (client *Client) {
	connConfig := &ConnConfig{
		RxStreamMaxChunkSize: 4 * 1024 * 1024,
		RxHeaderMaxLen:       1024,
		RxStructuredMaxLen:   64 * 1024,
		TxChunkSize:          4,
		Timeout: 10*time.Second,
		SendHeartbeatInterval: 5*time.Second,
	}
	clientConfig := &ClientConfig{
		ConnConfig: connConfig,
	}

	// start server before client to avoid deadlock
	go func() {
		ctx := ContextWithLogger(context.Background(), testingLogger{t})
		serveResult <- ServeConn(ctx, serverConn, connConfig, handler)
		t.Log("serving done")
	}()

	client, err := NewClientOnConn(clientConn, clientConfig)
	if err != nil {
		panic(err)
	}
	if serveResult == nil {
		serveResult = make(chan error, 1) // just forget it
	}
	return client
}

func testClientServerMockConns(t *testing.T, clientConn, serverConn net.Conn, handler HandlerFunc) (client *Client) {
	return testClientServerMockConnsServeResult(t, clientConn, serverConn, nil, handler)
}

func testClientServer(t *testing.T, handler HandlerFunc) (client *Client) {
	clientConn, serverConn := newTestPipe()
	return testClientServerMockConns(t, clientConn, serverConn, handler)
}

func TestBehaviorHandlerError(t *testing.T) {

	client := testClientServer(t, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return nil, nil, errors.New("test error")
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("foo"), nil)
	assert.Nil(t, stru)
	assert.Nil(t, stre)
	reperr, ok := err.(*RemoteEndpointError)
	require.True(t, ok)
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

	client := testClientServer(t, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
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
	client := testClientServer(t, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		defer reqStream.Close()
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

func TestBehaviorMultipleRequestsOnSameConnectionWork(t *testing.T) {

	client := testClientServer(t, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		assert.Equal(t, "foobar", endpoint)
		assert.Equal(t, "question", reqStructured.String())
		assert.Nil(t, reqStream)
		return bytes.NewBufferString("structured"), sReadCloser("stream"), nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))
	stre.Close()

	stru, stre, err = client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))
}

func TestBehaviorOpenStreamReturnsErrorOnOpenStream(t *testing.T) {

	client := testClientServer(t, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return bytes.NewBufferString("structured"), sReadCloser("stream"), nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	// do not consume stre
	_ = stre

	stru2, stre2, err2 := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("q2"), nil)
	assert.Nil(t, stru2)
	assert.Nil(t, stre2)
	assert.Error(t, err2)
	assert.Equal(t, ErrorRequestReplyWithOpenStream, err2)
}


func TestBehaviorConcurrentRequestReplyError(t *testing.T) {

	firstArrived , checkDone , firstDone := false, false, false
	client := testClientServer(t, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		firstArrived = true
		for !checkDone { runtime.Gosched() }
		// do not leave an open stream, that would cause other errors
		return bytes.NewBufferString("structured"), nil, nil
	})

	go func() {
		stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
		assert.NoError(t, err)
		assert.Equal(t, "structured", stru.String())
		assert.Nil(t, stre)
		firstDone = true
	}()

	for !firstArrived { runtime.Gosched() }

	stru2, stre2, err2 := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("q2"), nil)
	assert.Nil(t, stru2)
	assert.Nil(t, stre2)
	assert.Error(t, err2)
	assert.Equal(t, ErrorConcurrentRequestReply, err2)
	checkDone = true
	for !firstDone {}
}

type readWriteCloseRecorder struct {
	net.Conn
	closeCount int
}

func (r *readWriteCloseRecorder) Close() error {
	r.closeCount++
	return r.Conn.Close()
}

func TestBehaviorClientClosingUnconsumedStreamClosesConnection(t *testing.T) {

	clientConnP, serverConn := newTestPipe()
	clientConn := &readWriteCloseRecorder{clientConnP, 0}

	client := testClientServerMockConns(t, clientConn, serverConn, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return bytes.NewBufferString("structured"), sReadCloser("stream"), nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("q"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())

	err = stre.Close()
	assert.NoError(t, err)

	assert.Equal(t, 1, clientConn.closeCount)
}

func TestBehaviorServerClosesConnectionIfHandlerDoesNotCloseReqStream(t *testing.T) {

	clientConnP, serverConn := newTestPipe()
	clientConn := &readWriteCloseRecorder{clientConnP, 0}
	serveRes := make(chan error, 1)

	var handlerError = errors.New("handler error")
	client := testClientServerMockConnsServeResult(t, clientConn, serverConn, serveRes, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		// server does not close reqStream
		return nil, nil, handlerError
	})

	stru, stre, err := client.RequestReply(context.Background(), "foo", bytes.NewBufferString("q"), sReadCloser("this is never read"))
	assert.Nil(t, stru)
	assert.Nil(t, stre)
	expServerErr := &HandlerInputStreamNotClosedError{HandlerError: handlerError}
	expClientErr := &RemoteEndpointError{expServerErr.Error()}
	assert.Equal(t, expClientErr, err)
	assert.Equal(t, 1, clientConn.closeCount)
	assert.Equal(t, expServerErr, <-serveRes) // ServeConn should return, but from a protocol point of view, things went fine
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

	clientConn, serverConn := newTestPipe()
	serverRes := make(chan error, 1)

	mockStream := &mockReadCloser{bytes.NewBufferString("stream"), 0, 0}
	client := testClientServerMockConnsServeResult(t, clientConn, serverConn, serverRes, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
		return bytes.NewBufferString("structured"), mockStream, nil
	})

	stru, stre, err := client.RequestReply(context.Background(), "foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))

	// make server exit
	serverConn.Close()
	assert.Error(t, <-serverRes)
	assert.Equal(t, 1, mockStream.closeCount)
}

func TestBehaviorClientContextCancelClosesConnection(t *testing.T) {

	clientConnP, serverConn := newTestPipe()
	clientConn := &readWriteCloseRecorder{clientConnP, 0}
	serverErr := make(chan error, 1)

	serverReceivedReq, clientReturned, serverRespond := make(chan struct{}), make(chan struct{}), make(chan struct{})
	client := testClientServerMockConnsServeResult(t, clientConn, serverConn, serverErr, func(_ context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
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
		clientReturned <- struct{}{}
	}()

	<- serverReceivedReq
	cancel()
	<- clientReturned
	assert.Equal(t, 1, clientConn.closeCount)
	serverRespond <- struct{}{}
}
