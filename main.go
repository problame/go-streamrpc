package streamrpc

import (
	"bytes"
	"io"
	"github.com/problame/go-streamrpc/internal/pdu"
	"math"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"fmt"
	"net"
	"time"
	"context"
)

type ConnConfig struct {
	RxHeaderMaxLen uint32
	RxStructuredMaxLen uint32
	RxStreamMaxChunkSize uint32
	// FIXME enforce TxHeaderMaxLen, TxStructuredMaxLen on send path
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

// newConn performs the initial protocol handshake over c, and if successful, wraps c in the returned *Conn.
//
// Errors returned are either about invalid config or related to the protocol magic exchange (may include net errors).
//
// It is the callers responsibility to close c in case this function returns an error.
func newConn(c net.Conn, config *ConnConfig) (*Conn, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	conn := &Conn{
		c: c,
		config: config,
		closed: 0,
	}

	// use conn to get deadlines configured in config
	if err := pdu.WriteMagic(conn); err != nil {
		return nil, fmt.Errorf("protocol handshake failed (write): %s", err)
	}
	if err := pdu.ReadMagic(conn); err != nil {
		return nil, fmt.Errorf("protocol handshake failed (read): %s", err)
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
	if err := c.c.SetReadDeadline(c.config.RxTimeout.ProgressDeadline(time.Now())); err != nil {
		return 0, err
	}
	return c.c.Read(b)
}

func (c *Conn) Write(b []byte) (n int, err error) {
	if err := c.c.SetWriteDeadline(c.config.TxTimeout.ProgressDeadline(time.Now())); err != nil {
		return 0, err
	}
	return c.c.Write(b)
}

var _ BuffersWriter = &Conn{}

func (c *Conn) WriteBuffers(buffers *net.Buffers) (n int64, err error) {
	if err := c.c.SetWriteDeadline(c.config.TxTimeout.ProgressDeadline(time.Now())); err != nil {
		return 0, err
	}
	return io.Copy(c.c, buffers)
}

// Stream is a io.ReadCloser that provides access to the streamed part of a PDU packet.
// A Stream must always be fully consumed, i.e., read until an error is returned or be closed.
// While a Stream is not closed, the Stream's Conn's methods recv() and send() return errors.
type Stream struct {
	closed int32 // 0 = open; 1 = closed
	r *streamReader
	closeConnOnCloseAndUnconsumed bool
	conn *Conn
}

// Read implements io.Reader for Stream.
// It may return a *StreamError as error.
func (s *Stream) Read(p []byte) (n int, err error) {
	closed, consumed := s.State()
	if closed || consumed {
		return 0, io.EOF
	}
	return s.r.Read(p)
}

func (s *Stream) State() (closed, consumed bool) {
	closed = atomic.LoadInt32(&s.closed) == 1
	consumed = s.r.Consumed()
	return
}
func (s *Stream) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	if s.closeConnOnCloseAndUnconsumed && !s.r.Consumed() {
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

func (c *Conn) recv(ctx context.Context) (*recvResult) {

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
	if err := pdu.UnmarshalHeader(buf.Bytes(), &unmarshHeader); err != nil {
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
			r: newStreamReader(c, c.config.RxStreamMaxChunkSize),
		}
	} else {
		resStream = nil
	}

	return &recvResult{&unmarshHeader, resStructured, resStream, nil}
}

// fills in PayloadLen and Stream of pdu.Header
func (c *Conn) send(ctx context.Context, h *pdu.Header, reqStructured *bytes.Buffer, reqStream io.Reader) error {

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

	hdr, err := h.Marshal()
	if err != nil {
		return err
	}
	if len(hdr) > math.MaxUint32 {
		return errors.New("marshaled header longer than allowed by protocol")
	}
	var hdrLenBuf [4]byte
	binary.BigEndian.PutUint32(hdrLenBuf[:], uint32(len(hdr)))

	// write it all out
	bufsOnStack := [3][]byte{hdrLenBuf[:], hdr, reqStructured.Bytes()}
	bufs := net.Buffers(bufsOnStack[:])
	if h.PayloadLen == 0 {
		// avoid that WriteBuffers makes a write attempt for empty bytes
		bufs = bufs[0:2]
	}
	logger(ctx).Infof("Conn.send: write out sized part")
	if _, err := c.WriteBuffers(&bufs); err != nil {
		return err
	}
	if (reqStream != nil) {
		logger(ctx).Infof("Conn.send: write stream")
		if err := writeStream(ctx, c, reqStream, c.config.TxChunkSize); err != nil {
			return err
		}
	}
	logger(ctx).Infof("Conn.send: done")

	return nil
}





