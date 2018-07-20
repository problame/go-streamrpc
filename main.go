package streamrpc

import (
	_ "github.com/problame/go-streamrpc/internal/pdu"
	"bytes"
	"io"
	"github.com/problame/go-streamrpc/internal/pdu"
	"math"
	"github.com/golang/protobuf/proto"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"fmt"
	"net"
	"time"
)

type ConnConfig struct {
	RxHeaderMaxLen uint32
	RxStructuredMaxLen uint32
	RxStreamMaxChunkSize uint32
	TxChunkSize uint32

	RxTimeout, TxTimeout Timeout
}

type Timeout struct {
	// The time allotted to a Read or Write until it must have made > 0 bytes of progress
	Progress time.Duration
}

func (t *Timeout) ProgressDeadline(now time.Time) time.Time {
	if t.Progress == 0 {
		return time.Time{}
	}
	return now.Add(t.Progress)
}

func (c *ConnConfig) Validate() error {
	if c.TxChunkSize <= 0 {
		return errors.New("TxChunkSize must be greater than 0")
	}
	if c.RxHeaderMaxLen <= 0 {
		return errors.New("RxHeaderMaxLen must be greater than 0")
	}
	if c.RxStructuredMaxLen <= 0 {
		return errors.New("RxStructuredMaxLen must be greater than 0")
	}
	if c.RxStreamMaxChunkSize <= 0 {
		return errors.New("RxStreamMaxChunkSize must be greater than 0")
	}
	return nil
}

// Conn gates access to the underlying io.ReadWriteCloser to ensure that we always speak correct wire protocol.
// FIXME this is totally internal, right?
//
// recv and send may be called concurrently, e.g. to receive an early error during a long send operation.
// However, this only works because we assume that the wrapped io.ReadWriteCloser Conn.c will return errors on
// Write or Read after it was Closed. This is the behavior of net.Conn.
type Conn struct {
	c        net.Conn
	closed   int32 // 0 = open, 1 = closed
	config   *ConnConfig
	recvBusy cas // 0 usable, 1 = recv running, 2 = stream closed
	sendBusy spinlock
	lastReadDL, lastWriteDL time.Time
}

// newConn only returns errors returned by config.Validate()
func newConn(c net.Conn, config *ConnConfig) (*Conn, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	conn := &Conn{
		c: c,
		config: config,
		closed: 0,
	}
	return conn, nil
}

func (c *Conn) Closed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// Close calls the underlying io.ReadWriteCloser's Close exactly once and invalidates Conn
// (c.Closed will return true afterwards)
func (c *Conn) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		// someone else closed it already
		return nil
	}
	err := c.c.Close()
	return err
}

func (c *Conn) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, io.ErrShortBuffer
	}
	n = 0
	firstRound := true
	for ;; firstRound = false {
		now := time.Now()
		if now.After(c.lastReadDL) { // also branches for c.lastReadDL = 0, but it makes control flow less ugly
			dl := c.config.RxTimeout.ProgressDeadline(now)
			if !dl.IsZero() {
				if err := c.c.SetReadDeadline(dl); err != nil {
					return 0, err
				}
				c.lastReadDL = dl
			}
		}
		ni, err := c.c.Read(b[n:])
		n += ni
		if !c.lastReadDL.IsZero() && err != nil && (firstRound || ni > 0) { // last parentheses = did we make progress?
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				continue
			}
		}
		return n, err
	}
}

func (c *Conn) WriteBuffers(buffers *net.Buffers) (n int64, err error) {
	if len(*buffers) == 0 {
		return 0, io.ErrShortBuffer
	}
	n = 0
	firstRound := true
	for ;; firstRound = false {
		now := time.Now()
		if now.After(c.lastWriteDL) { // also branches for c.lastReadDL = 0, but it makes control flow less ugly
			dl := c.config.TxTimeout.ProgressDeadline(now)
			if !dl.IsZero() {
				if err := c.c.SetWriteDeadline(dl); err != nil {
					return 0, err
				}
				c.lastWriteDL = dl
			}
		}
		ni, err := io.Copy(c.c, buffers)
		n += ni
		if !c.lastWriteDL.IsZero() && err != nil && (firstRound || ni > 0) { // last parentheses = did we make progress?
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				continue
			}
		}
		return n, err
	}
}

func (c *Conn) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, io.ErrShortBuffer
	}
	n = 0
	firstRound := true
	for ;; firstRound = false {
		now := time.Now()
		if now.After(c.lastWriteDL) { // also branches for c.lastReadDL = 0, but it makes control flow less ugly
			dl := c.config.TxTimeout.ProgressDeadline(now)
			if !dl.IsZero() {
				if err := c.c.SetWriteDeadline(dl); err != nil {
					return 0, err
				}
				c.lastWriteDL = dl
			}
		}
		ni, err := c.c.Write(b[n:])
		n += ni
		if !c.lastWriteDL.IsZero() && err != nil && (firstRound || ni > 0) { // last parentheses = did we make progress?
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				continue
			}
		}
		return n, err
	}
}

// Stream is a io.ReadCloser that provides access to the streamed part of a PDU packet.
// A Stream must always be fully consumed, i.e., read until an error is returned or be closed.
// While a Stream is not closed, the Stream's Conn's methods recv() and send() return errors.
type Stream struct {
	closed int32 // 0 = open; 1 = closed
	r *streamReader
	conn *Conn
}

// Read implements io.Reader for Stream.
// It may return a *StreamError as error.
func (s *Stream) Read(p []byte) (n int, err error) {
	return s.r.Read(p)
}

func (s *Stream) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	if !s.r.Consumed() {
		// There are still chunks on Conn that belong to this stream,
		// hence the Conn is in unknown state and must be closed.
		s.conn.Close()
	}
	if p := s.conn.recvBusy.CompareAndSwap(2, 0); p != 2 {
		panic(fmt.Sprintf("inconsistent use of recvBusy: %v", p))
	}
	// only after the stream (and connection, if necessary) is closed can a new recv start
	return nil
}

type recvResult struct {
	header     *pdu.Header
	structured *bytes.Buffer
	stream     *Stream
	err        error
}

var (
	errorConcurrentRecv = errors.New("concurrent recv on busy connection")
	errorRecvWithOpenStream = errors.New("recv with open result stream")
	errorConcurrentSend= errors.New("concurrent send on busy connection")
)

func (c *Conn) recv() (*recvResult) {

	if c.Closed() {
		return &recvResult{nil, nil, nil, errors.New("recv on closed connection")}
	}

	switch c.recvBusy.CompareAndSwap(0, 1) {
	case 1:
		return &recvResult{nil, nil, nil, errorConcurrentRecv}
	case 2:
		return &recvResult{nil, nil, nil, errorRecvWithOpenStream}
	default:
	}
	unlockInStream := false
	defer func() {
		if !unlockInStream {
			if p := c.recvBusy.CompareAndSwap(1, 0); p != 1 {
				panic(fmt.Sprintf("inconsistent use of recvBusy: %v", p))
			}
		}
	}()

	var hdrLen uint32
	if err := binary.Read(c, binary.BigEndian, &hdrLen); err != nil {
		return &recvResult{nil, nil, nil, err}
	}

	if hdrLen > c.config.RxHeaderMaxLen {
		return &recvResult{nil, nil, nil, errors.New("reply header exceeds allowed length")}
	}

	buf := bytes.NewBuffer(make([]byte, 0, hdrLen))
	_, err := io.CopyN(buf, c, int64(hdrLen))
	if err != nil {
		return &recvResult{nil, nil, nil, err}
	}

	var unmarshHeader pdu.Header
	if err := proto.Unmarshal(buf.Bytes(), &unmarshHeader); err != nil {
		return &recvResult{nil, nil, nil, errors.New("could not unmarshal header")}
	}

	if unmarshHeader.PayloadLen > c.config.RxStructuredMaxLen {
		return &recvResult{&unmarshHeader, nil, nil, errors.New("reply structured part exceeds allowed length")}
	}

	resStructured := bytes.NewBuffer(make([]byte, 0, unmarshHeader.PayloadLen))
	_, err = io.CopyN(resStructured, c, int64(unmarshHeader.PayloadLen))
	if err != nil {
		return &recvResult{&unmarshHeader, nil, nil, err}
	}

	var resStream *Stream
	if unmarshHeader.Stream {
		unlockInStream = true
		if p := c.recvBusy.CompareAndSwap(1, 2); p != 1 {
			panic(fmt.Sprintf("inconsistent use of recvBusy: %v", p))
		}
		resStream = &Stream {
			conn: c,
			r: newStreamReader(c.c, c.config.RxStreamMaxChunkSize),
		}
	} else {
		resStream = nil
	}

	return &recvResult{&unmarshHeader, resStructured, resStream, nil}
}

// fills in PayloadLen and Stream of pdu.Header
func (c *Conn) send(h *pdu.Header, reqStructured *bytes.Buffer, reqStream io.Reader) error {

	if c.Closed() {
		return errors.New("send on closed connection")
	}

	if !c.sendBusy.TryLock() {
		return errorConcurrentSend
	}
	defer c.sendBusy.Unlock()

	if reqStructured == nil {
		reqStructured = bytes.NewBuffer([]byte{})
	}
	if reqStructured.Len() > math.MaxUint32 {
		return errors.New("structured part of request exceeds maximum length")
	}

	h.PayloadLen = uint32(reqStructured.Len())
	h.Stream = reqStream != nil

	hdr, err := proto.Marshal(h)
	if err != nil {
		return err
	}
	if len(hdr) > math.MaxUint32 {
		return errors.New("marshaled header longer than allowed by protocol")
	}

	// send it all out

	if err = binary.Write(c.c, binary.BigEndian, uint32(len(hdr))); err != nil {
		return err
	}
	if _, err = io.Copy(c.c, bytes.NewReader(hdr)); err != nil {
		return err
	}
	if _, err = io.Copy(c.c, reqStructured); err != nil {
		return err
	}
	if (reqStream != nil) {
		if err := writeStream(c.c, reqStream, c.config.TxChunkSize); err != nil {
			return err
		}
	}

	return nil
}





