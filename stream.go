package streamrpc

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"strings"
	"errors"
)

// protocol constants, do not touch
const (
	STATUS_OK    uint8 = 0
	STATUS_EOF   uint8 = 1
	STATUS_ERROR uint8 = 2
)

type SourceStreamError struct {
	StreamError error
}

func (e *SourceStreamError) Error() string {
	return e.StreamError.Error()
}

// does not return an error if r returns an error
func writeStream(out io.Writer, r io.Reader, csiz uint32) error {

	bufStorage := make([]byte, 0, csiz)
	cbuf := bytes.NewBuffer(bufStorage)

	writeChunkHeader := func(len uint32, status uint8) error {
		// write LEN
		if err := binary.Write(out, binary.BigEndian, len); err != nil {
			return err
		}
		// write status
		if err := binary.Write(out, binary.BigEndian, status); err != nil {
			return err
		}
		return nil
	}

	for {
		n, err := io.CopyN(cbuf, r, int64(csiz))

		if n < 0 || n > int64(csiz) || n > math.MaxUint32 {
			panic("implementation error")
		}

		if err != nil && err != io.EOF {
			errmsg := err.Error()
			streamErr := err

			var errChunkSizeBuf bytes.Buffer
			io.Copy(&errChunkSizeBuf, io.LimitReader(strings.NewReader(errmsg), int64(csiz))) // cannot fail
			if err := writeChunkHeader(uint32(errChunkSizeBuf.Len()), STATUS_ERROR); err != nil {
				return err
			}
			if _, err = io.Copy(out, &errChunkSizeBuf); err != nil {
				return err
			}
			return &SourceStreamError{streamErr}
		} else if err == io.EOF {
			if err := writeChunkHeader(uint32(n), STATUS_OK); err != nil {
				return err
			}
			if _, err := io.Copy(out, cbuf); err != nil {
				return err
			}
			if err := writeChunkHeader(0, STATUS_EOF); err != nil {
				return err
			}
			return nil
		}

		// err == nil
		if err := writeChunkHeader(uint32(n), STATUS_OK); err != nil {
			return err
		}
		// flush out cbuf to disk
		n, err = io.Copy(out, cbuf)
		if err != nil { // assume the error is never due to cbuf
			return err
		}

	}
	panic("implementation error")
	return nil
}

type streamReader struct {
	s              io.Reader
	mcsiz          uint32
	chunkRemaining uint32
	e              error
}

func newStreamReader(r io.Reader, macChunkSize uint32) *streamReader {
	return &streamReader{
		s: r,
		mcsiz: macChunkSize,
	}
}

var ChunkSizeExceededError = errors.New("stream chunk exceeds maximum chunk size")

var ChunkHeaderUnknownStatusError = errors.New("received unknown status in stream chunk header")

type StreamError struct {
	msg string
}

func (e *StreamError) Error() string {
	return e.msg
}

func (r *streamReader) Read(p []byte) (n int, err error) {

	if r.e != nil {
		return 0, r.e
	}

	n = 0

	if r.chunkRemaining > 0 {
		n, err = io.LimitReader(r.s, int64(r.chunkRemaining)).Read(p)
		if err != nil && err != io.EOF {
			return n, err
		}
		r.chunkRemaining -= uint32(n)
		return n, nil
	}

	// read chunk header
	var len uint32
	var status uint8
	if err := binary.Read(r.s, binary.BigEndian, &len); err != nil {
		r.e = err
		return 0, err
	}
	if err := binary.Read(r.s, binary.BigEndian, &status); err != nil {
		r.e = err
		return 0, err
	}

	if len > r.mcsiz {
		r.e = ChunkSizeExceededError
		return 0, r.e
	}

	if status == STATUS_OK {
		r.chunkRemaining = len
		return r.Read(p)
	}

	if status == STATUS_EOF {
		r.e = io.EOF
		return 0, r.e
	}

	if status == STATUS_ERROR {
		buf := bytes.NewBuffer(make([]byte, 0, len))
		_, err := io.CopyN(buf, r.s, int64(len))
		if err != nil {
			r.e = err
			return 0, r.e
		}
		r.e = &StreamError{buf.String()}
		return 0, r.e
	}

	return 0, ChunkHeaderUnknownStatusError
}

func (r *streamReader) Consumed() bool {
	_, isStreamError := r.e.(*StreamError)
	return r.e == io.EOF || isStreamError
}
