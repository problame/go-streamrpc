package streamrpc

import "bytes"

type bufferRWC struct {
	bytes.Buffer
}

func (bufferRWC) Close() error {
	return nil
}
