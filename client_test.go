package streamrpc

import (
	"testing"
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"context"
)

func TestClientServer_Basic(t *testing.T) {

	connConfig := ConnConfig{
		RxStreamMaxChunkSize: 4*1024*1024,
		RxHeaderMaxLen: 1024,
		RxStructuredMaxLen: 64*1024,
		TxChunkSize: 4,
	}

	clientConn, serverConn := net.Pipe()

	go ServeConn(serverConn, &connConfig, func(endpoint string, reqStructured *bytes.Buffer, reqStream io.Reader) (*bytes.Buffer, io.Reader, error) {
		return bytes.NewBufferString("this is the structured response"), bytes.NewBufferString("this is the streamed response"), nil
	})
	client, err :=  NewClientOnConn(clientConn, &connConfig)
	assert.NoError(t, err)

	in := bytes.NewBufferString("this is a test")
	stream := bytes.NewBufferString("this is a stream")

	out, outstream, err := client.RequestReply(context.Background(), "foobar", in, stream)
	assert.Nil(t, err)

	assert.Equal(t, "this is the structured response", out.String())

	var outstreamBuf bytes.Buffer
	_, err = io.Copy(&outstreamBuf, outstream)
	assert.Nil(t, err)
	assert.Equal(t, "this is the streamed response", outstreamBuf.String())

}

