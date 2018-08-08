package pdu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Header is en/decoded with encoding/json
type Header struct {
	PayloadLen    uint32
	Stream        bool
	Endpoint      string
	EndpointError string
	Close         bool
}

func (h *Header) Marshal() ([]byte, error) {
	return json.Marshal(h)
}

func UnmarshalHeader(buf []byte, h *Header) error {
	return json.Unmarshal(buf, &h)
}

var (
	// MagicConstant is the first 15 bytes of the 16 byte magic.
	MagicConstant []byte = []byte{
		0x18, 0xce, 0x72, 0x48, 0x4e, 0x61, 0x61, 0x1d,
		0x71, 0xeb, 0x03, 0xa8, 0x3c, 0xe2, 0x6f,
	}
	// CurrentVersion is the current protocol version (last 1b of magic).
	CurrentVersion uint8 = 1
)

// Writes magic with CurrentVersion to w.
// Any errors returned are io errors on w.
func WriteMagic(w io.Writer) error {
	m := newMagic(CurrentVersion)
	b := bytes.NewBuffer(m)
	_, err := io.Copy(w, b)
	return err
}

// Returns InvalidMagic if the constant part of magic does not match.
// Returns an instance of *VersionNumberError if the version number does
// not match CurrentVersion.
func ReadMagic(r io.Reader) error {
	var buf [16]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	if !bytes.Equal(buf[0:15], MagicConstant) {
		return InvalidMagic
	}
	version := buf[15]
	if version != CurrentVersion {
		return &VersionNumberError{CurrentVersion, version}
	}
	return nil
}

func newMagic(version uint8) []byte {
	b := [16]byte{}
	copy(b[0:15], MagicConstant[:])
	b[15] = version
	return b[:]
}

var (
	InvalidMagic = errors.New("invalid magic number")
)

type VersionNumberError struct {
	expected, received uint8
}

func (m *VersionNumberError) Error() string {
	return fmt.Sprintf(
		"protocol version numbers do not match: received %d, expected %d",
		m.received, m.expected)
}
