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
)

func testClientServer(handler HandlerFunc) (client *Client) {

	connConfig := ConnConfig{
		RxStreamMaxChunkSize: 4 * 1024 * 1024,
		RxHeaderMaxLen:       1024,
		RxStructuredMaxLen:   64 * 1024,
		TxChunkSize:          4,
	}

	clientConn, serverConn := net.Pipe()

	client = NewClientOnConn(clientConn, &connConfig)
	go ServeConn(serverConn, &connConfig, handler)
	return client
}

func TestBehaviorHandlerError(t *testing.T) {

	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (resStructured *bytes.Buffer, resStream io.Reader, err error) {
		return nil, nil, errors.New("test error")
	})

	stru, stre, err := client.RequestReply("foobar", bytes.NewBufferString("foo"), nil)
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

	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (resStructured *bytes.Buffer, resStream io.Reader, err error) {
		assert.Equal(t, "foobar", endpoint)
		assert.Equal(t, "question", reqStructured.String())
		assert.Nil(t, reqStream)
		return bytes.NewBufferString("structured"), bytes.NewBufferString("stream"), nil
	})

	stru, stre, err := client.RequestReply("foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)

	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))

}

func TestBehaviorStreamRequestReply(t *testing.T) {
	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (resStructured *bytes.Buffer, resStream io.Reader, err error) {
		assert.Equal(t, "foobar", endpoint)
		assert.Equal(t, "question", reqStructured.String())
		assert.Equal(t, "stream", readerToString(reqStream))
		return bytes.NewBufferString("structured"), nil, nil
	})

	stru, stre, err := client.RequestReply("foobar", bytes.NewBufferString("question"), bytes.NewBufferString("stream"))
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Nil(t, stre)
}

func TestBehaviorOpenStreamBlocksNextRequest(t *testing.T) {

	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (resStructured *bytes.Buffer, resStream io.Reader, err error) {
		return bytes.NewBufferString("structured"), bytes.NewBufferString("stream"), nil
	})

	stru, stre, err := client.RequestReply("foobar", bytes.NewBufferString("question"), nil)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())

	var secondRequestDone int32
	go func(){
		atomic.StoreInt32(&secondRequestDone, 1)
		stru, stre, err := client.RequestReply("foobar", bytes.NewBufferString("q2"), nil)
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

	mockStream := &mockReadCloser{bytes.NewBufferString("stream"), 0, 0}
	client := testClientServer(func(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (resStructured *bytes.Buffer, resStream io.Reader, err error) {
		return bytes.NewBufferString("structured"), mockStream, nil
	})

	stru, stre, err := client.RequestReply("foobar", bytes.NewBufferString("question"), nil)
	time.Sleep(100*time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, "structured", stru.String())
	assert.Equal(t, "stream", readerToString(stre))
	assert.Equal(t, 1, mockStream.closeCount)
}