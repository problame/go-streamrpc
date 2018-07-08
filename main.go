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
	"sync"
	"sync/atomic"
)

type ConnConfig struct {
	RxHeaderMaxLen uint32
	RxStructuredMaxLen uint32
	RxStreamMaxChunkSize uint32
	TxChunkSize uint32
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
	c        io.ReadWriteCloser
	cvalid   atomic.Value
	config   *ConnConfig
	recvBusy sync.Mutex
	sendBusy sync.Mutex
}

// newConn only returns errors returned by config.Validate()
func newConn(c io.ReadWriteCloser, config *ConnConfig) (*Conn, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	conn := &Conn{
		c: c,
		config: config,
	}
	conn.cvalid.Store(true)
	return conn, nil
}

func (c *Conn) Valid() bool {
	if c == nil {
		return false
	}
	if c.c == nil {
		return false
	}
	return c.cvalid.Load().(bool)
}

// Close calls the underlying io.ReadWriteCloser's Close once and invalidates that Conn
// (c.Valid will return false afterwards)
func (c *Conn) Close() error {
	c.cvalid.Store(false)
	err := c.c.Close()
	return err
}

// Stream is a io.ReadCloser that provides access to the streamed part of a PDU packet.
// A Stream must always be fully consumed, i.e., read until an error is returned or be closed.
// Before a Stream is consumed, no new requests can be sent over the connection, and callers will block.
type Stream struct {
	r *streamReader
	conn *Conn
}

// Read implements io.Reader for Stream.
// It may return a *StreamError as error.
func (s *Stream) Read(p []byte) (n int, err error) {
	n, err = s.r.Read(p)
	if err != nil {
		s.Close()
		s.conn.recvBusy.Unlock()
		s.r = nil
		s.conn = nil
	}
	return n, err
}

func (s *Stream) Close() error {
	if s == nil {
		return nil
	}
	if !s.r.Consumed() {
		// There are still chunks on Conn that belong to this stream,
		// hence the Conn is in unknown state and must be closed.
		s.conn.Close()
	}
	return nil
}

type recvResult struct {
	header     *pdu.Header
	structured *bytes.Buffer
	stream     *Stream
	err        error
}

func (c *Conn) recv() (*recvResult) {
	c.recvBusy.Lock()
	unlockInStream := false
	defer func() {
		if !unlockInStream {
			c.recvBusy.Unlock()
		}
	}()
	// do not unlock, this is done in Read() or Close() of Stream

	var hdrLen uint32
	if err := binary.Read(c.c, binary.BigEndian, &hdrLen); err != nil {
		return &recvResult{nil, nil, nil, err}
	}

	if hdrLen > c.config.RxHeaderMaxLen {
		return &recvResult{nil, nil, nil, errors.New("reply header exceeds allowed length")}
	}

	buf := bytes.NewBuffer(make([]byte, 0, hdrLen))
	_, err := io.CopyN(buf, c.c, int64(hdrLen))
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
	_, err = io.CopyN(resStructured, c.c, int64(unmarshHeader.PayloadLen))
	if err != nil {
		return &recvResult{&unmarshHeader, nil, nil, err}
	}

	var resStream *Stream
	if unmarshHeader.Stream {
		unlockInStream = true
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

	c.sendBusy.Lock()
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





