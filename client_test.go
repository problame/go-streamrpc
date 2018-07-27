package streamrpc

import (
	"testing"
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"context"
	"github.com/stretchr/testify/require"
)

func TestClientServer_Basic(t *testing.T) {

	clientConf := &ClientConfig{
		ConnConfig: &ConnConfig{
			RxStreamMaxChunkSize: 4*1024*1024,
			RxHeaderMaxLen: 1024,
			RxStructuredMaxLen: 64*1024,
			TxChunkSize: 4,
		},
	}

	clientConn, serverConn := net.Pipe()
	ctx := context.WithValue(context.Background(), ContextKeyLogger, testingLogger{t})
	go ServeConn(ctx, serverConn, clientConf.ConnConfig, func(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (*bytes.Buffer, io.ReadCloser, error) {
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
