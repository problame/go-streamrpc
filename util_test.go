package streamrpc

import (
	"bytes"
	"io"
)

type bufferRWC struct {
	*bytes.Buffer
}

func (bufferRWC) Close() error {
	return nil
}

func sReadCloser(s string) io.ReadCloser {
	return bufferRWC{bytes.NewBufferString(s)}
}

