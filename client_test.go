package streamrpc

import (
	"testing"
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"time"
	"math"
)

func TestClientServer_Basic(t *testing.T) {

	clientConf := &ClientConfig{
		MaxConnectAttempts: 1,
		ReconnectBackoffBase: 10*time.Millisecond,
		ReconnectBackoffFactor: 1,
		ConnConfig: &ConnConfig{
			RxStreamMaxChunkSize: 4*1024*1024,
			RxHeaderMaxLen: 1024,
			RxStructuredMaxLen: 64*1024,
			TxChunkSize: 4,
		},
	}

	clientConn, serverConn := net.Pipe()

	go ServeConn(serverConn, clientConf.ConnConfig, func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (*bytes.Buffer, io.ReadCloser, error) {
		return bytes.NewBufferString("this is the structured response"), sReadCloser("this is the streamed response"), nil
	})
	client, err :=  NewClientOnConn(clientConn, clientConf)
	require.NoError(t, err)

	in := bytes.NewBufferString("this is a test")
	stream := sReadCloser("this is a stream")

	out, outstream, err := client.RequestReply(context.Background(), "foobar", in, stream)
	require.Nil(t, err)

	assert.Equal(t, "this is the structured response", out.String())

	var outstreamBuf bytes.Buffer
	_, err = io.Copy(&outstreamBuf, outstream)
	assert.Nil(t, err)
	assert.Equal(t, "this is the streamed response", outstreamBuf.String())

}

type mockConnecter struct {
	mockConn net.Conn
	mockErr error
}

func (m *mockConnecter) Connect(ctx context.Context) (net.Conn, error) {
	return m.mockConn, m.mockErr
}

type mockLoggerCalls struct {
	method string
	time time.Time
	fmt string
	args []interface{}
}
type mockLogger struct {
	calls []mockLoggerCalls
}

func (m *mockLogger) Printf(fmt string, args... interface{}) {
	m.calls = append(m.calls, mockLoggerCalls{"Printf", time.Now(), fmt, args})
}

func TestClientReconnects(t *testing.T) {

	clientConf := &ClientConfig{
		MaxConnectAttempts:     6,
		ReconnectBackoffBase:   10*time.Millisecond,
		ReconnectBackoffFactor: 1.1,
		ConnConfig:  &ConnConfig{
			RxStreamMaxChunkSize: 4*1024*1024,
			RxHeaderMaxLen: 1024,
			RxStructuredMaxLen: 64*1024,
			TxChunkSize: 4,
		},
	}

	cnErr := errors.New("ooGhoZ8ahmaguVeiy2Ciephep1Weit")
	cn := &mockConnecter{nil, cnErr}
	c, err := NewClient(cn, clientConf)
	require.NoError(t, err)

	l := mockLogger{}
	ctx := context.WithValue(context.Background(), ContextKeyLogger, &l)
	_, _, err = c.RequestReply(ctx, "foobar", bytes.NewBufferString("structured"), nil)
	require.Error(t, err)
	assert.Equal(t, ErrorMaxReconnects, err, "reconn already logs the individual errors")

	require.True(t, len(l.calls) > 1)
	errorLog := make([]mockLoggerCalls, 0, len(l.calls))
	for _, c := range l.calls {
		for _, a := range c.args {
			if a == cnErr {
				errorLog = append(errorLog, c)
				t.Logf("error log entry found: %#v", c)
				break
			}
		}
	}
	require.Equal(t, clientConf.MaxConnectAttempts, len(errorLog))

	// We don't want to mock package time in Client.reconn(), thus error log must serve us here
	var lastSleep time.Duration
	var totalSleep time.Duration
	for i := 1; i < len(errorLog); i++ {
		thisSleep := errorLog[i].time.Sub(errorLog[i-1].time)
		totalSleep += thisSleep
		t.Logf("sleep %v was %v", i, thisSleep.Seconds())
		if i == 1 {
			assert.True(t, thisSleep >= clientConf.ReconnectBackoffBase)
		} else {
			tss := thisSleep.Seconds()
			ess := clientConf.ReconnectBackoffFactor*lastSleep.Seconds()
			if !(math.Abs(tss - ess) < 0.030) { // episolon for tick-based timers in kernels + sched latency
				t.Errorf("sleep %v expected to be %v but was %v", i, ess, tss)
			}
		}
		lastSleep = thisSleep
	}

	// test client to be re-usable
	before := time.Now()
	_, _, err = c.RequestReply(ctx, "foobar", bytes.NewBufferString("structured"), nil)
	secondDuration := time.Now().Sub(before)
	require.Error(t, err)
	assert.Equal(t, ErrorMaxReconnects, err, "reconn already logs the individual errors")
	assert.True(t, math.Abs(secondDuration.Seconds() - totalSleep.Seconds()) < 0.030)

	// test context handling
	ctx, _ = context.WithTimeout(ctx, totalSleep/2)
	before = time.Now()
	_, _, err = c.RequestReply(ctx, "foobar", bytes.NewBufferString("structured"), nil)
	cancelDuration := time.Now().Sub(before)
	require.Error(t, err)
	assert.True(t, math.Abs((2*cancelDuration - totalSleep).Seconds()) < 0.030, "cancels should interrupt reconnection")
	assert.Equal(t, context.DeadlineExceeded, err, "cancelled or timed out context should return ctx error")


}
