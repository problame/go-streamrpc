package streamrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
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

type chunkBuffer struct {
	csiz               uint32
	headerLastChunkLen uint32
	chunkLastReadLen   int
	header             []byte
	chunk              []byte
}

func newChunkBuffer(csiz uint32) chunkBuffer {
	cbuf := chunkBuffer{
		csiz:   csiz,
		header: make([]byte, 5),
		chunk:  make([]byte, csiz),
	}
	return cbuf
}

func (b *chunkBuffer) prependHeader(len uint32, status uint8) {
	// write LEN
	binary.BigEndian.PutUint32(b.header[0:4], len)
	// write status
	b.header[4] = status
	b.headerLastChunkLen = len
}

func (b *chunkBuffer) readChunk(r io.Reader) (int64, error) {
	if b.chunkLastReadLen != 0 {
		panic("can only read once before needing to flush")
	}
	n, err := r.Read(b.chunk)
	b.chunkLastReadLen = n
	return int64(n), err
}

// BuffersWriter is a public version of package net's buffersWriter.
// TODO: Report the requirement for a public BuffersWriter to golang team, wait until implemented, then remove this
type BuffersWriter interface {
	WriteBuffers(buffers *net.Buffers) (int64, error)
}

func (b *chunkBuffer) flush(w io.Writer) error {
	if int(b.headerLastChunkLen) != b.chunkLastReadLen {
		panic("chunk length specified in header is inconsistent with last readChunk")
	}
	expLen := int64(len(b.header)) + int64(b.chunkLastReadLen)
	iov := net.Buffers{b.header[:], b.chunk[:b.chunkLastReadLen]}
	var (
		n   int64
		err error
	)
	if bw, ok := w.(BuffersWriter); ok {
		n, err = bw.WriteBuffers(&iov)
	} else {
		// use the iov.WriteTo anyways, maybe w is a net.buffersWriter after all
		n, err = iov.WriteTo(w)
	}
	if err != nil {
		return err
	}
	if n != expLen {
		return io.ErrShortWrite
	}
	b.chunkLastReadLen = 0
	return nil
}

// does not return an error if r returns an error
func writeStream(ctx context.Context, out io.Writer, r io.Reader, csiz uint32) error {

	cbuf := newChunkBuffer(csiz)

	for {
		n, err := cbuf.readChunk(r)

		if err != nil && err != io.EOF {
			errmsg := err.Error()
			streamErr := err
			logger(ctx).Infof("writeStream: source error: %s", streamErr)

			errChunk := newChunkBuffer(csiz)
			n, err := errChunk.readChunk(strings.NewReader(errmsg))
			if err != nil {
				return err
			}
			errChunk.prependHeader(uint32(n), STATUS_ERROR)
			if err := errChunk.flush(out); err != nil {
				return err
			}
			return &SourceStreamError{streamErr}
		} else if err == io.EOF {
			logger(ctx).Infof("writeStream: source consumed (io.EOF reached)")
			cbuf.prependHeader(uint32(n), STATUS_OK)
			if err := cbuf.flush(out); err != nil {
				return err
			}
			cbuf.prependHeader(0, STATUS_EOF)
			if err := cbuf.flush(out); err != nil {
				return err
			}
			return nil
		}

		// err == nil
		cbuf.prependHeader(uint32(n), STATUS_OK)
		if err := cbuf.flush(out); err != nil { // assume the error is never due to cbuf's chunkData buffer
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
		s:     r,
		mcsiz: macChunkSize,
	}
}

var ChunkSizeExceededError = errors.New("stream chunk exceeds maximum chunk size")

var ChunkHeaderUnknownStatusError = errors.New("received unknown status in stream chunk header")

// StreamError encodes an error received in the STATUS_ERROR trailer chunk.
type StreamError struct {
	msg string
}

func (e *StreamError) Error() string {
	return e.msg
}

// Read reads from the stream encoded on the underlying io.Reader.
// If the stream was aborted by the remote side with an error trailer (STATUS_ERROR)
// Read returns that trailer error as a *StreamError
func (r *streamReader) Read(p []byte) (n int, err error) {

	if r.e != nil {
		return 0, r.e
	}

restart:
	n = 0

	if r.chunkRemaining > 0 {
		lr := io.LimitedReader{R: r.s, N: int64(r.chunkRemaining)}
		n, err := lr.Read(p)
		if err != nil && err != io.EOF { // lr might return EOF even though r.s still has data
			r.e = err
			return n, err
		}
		if n == 0 && err == io.EOF {
			r.e = err
			return n, err
		}
		if n == 0 {
			// n == 0 means no progress, cannot let that happen?
			panic(fmt.Sprintf("%v %s", r.chunkRemaining, err))
		}
		r.chunkRemaining -= uint32(n)
		return n, nil
	}

	// read chunk header
	var (
		chunkLen uint32
		status   uint8
	)
	var hdrBuf []byte
	if len(p) >= 5 {
		hdrBuf = p[0:5]
	} else {
		var reserveBuf [5]byte
		hdrBuf = reserveBuf[0:5]
	}
	lr := io.LimitedReader{r.s, 5}
	if _, err := lr.Read(hdrBuf); err != nil {
		r.e = err
		return 0, r.e
	}
	chunkLen = binary.BigEndian.Uint32(hdrBuf[0:4])
	status = uint8(hdrBuf[4])

	if chunkLen > r.mcsiz {
		r.e = ChunkSizeExceededError
		return 0, r.e
	}

	if status == STATUS_OK {
		r.chunkRemaining = chunkLen
		goto restart
	}

	if status == STATUS_EOF {
		r.e = io.EOF
		return 0, r.e
	}

	if status == STATUS_ERROR {
		buf := bytes.NewBuffer(make([]byte, 0, chunkLen))
		_, err := io.CopyN(buf, r.s, int64(chunkLen))
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
