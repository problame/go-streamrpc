package streamrpc

import (
	"testing"
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
)

func TestClient_EchoRequestReply(t *testing.T) {

	connConfig := ConnConfig{
		RxStreamMaxChunkSize: 4*1024*1024,
		RxHeaderMaxLen: 1024,
		RxStructuredMaxLen: 64*1024,
		TxChunkSize: 4,
	}
	var buf bufferRWC
	client :=  NewClientOnConn(&buf, &connConfig)

	in := bytes.NewBufferString("this is a test")
	stream := bytes.NewBufferString("this is a stream")

	out, outstream, err := client.RequestReply("foobar", in, stream)
	assert.Nil(t, err)

	assert.Equal(t, "this is a test", out.String())

	var outstreamBuf bytes.Buffer
	_, err = io.Copy(&outstreamBuf, outstream)
	assert.Nil(t, err)
	assert.Equal(t, "this is a stream", outstreamBuf.String())

}

