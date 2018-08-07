package pdu

import (
	"encoding/json"
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
