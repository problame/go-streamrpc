// Code generated by protoc-gen-go. DO NOT EDIT.
// source: main.proto

/*
Package pdu is a generated protocol buffer package.

It is generated from these files:
	main.proto

It has these top-level messages:
	Header
*/
package pdu

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Header struct {
	PayloadLen    uint32 `protobuf:"varint,1,opt,name=PayloadLen" json:"PayloadLen,omitempty"`
	Stream        bool   `protobuf:"varint,2,opt,name=Stream" json:"Stream,omitempty"`
	Endpoint      string `protobuf:"bytes,3,opt,name=Endpoint" json:"Endpoint,omitempty"`
	EndpointError string `protobuf:"bytes,4,opt,name=EndpointError" json:"EndpointError,omitempty"`
	Close         bool   `protobuf:"varint,5,opt,name=Close" json:"Close,omitempty"`
}

func (m *Header) Reset()                    { *m = Header{} }
func (m *Header) String() string            { return proto.CompactTextString(m) }
func (*Header) ProtoMessage()               {}
func (*Header) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Header) GetPayloadLen() uint32 {
	if m != nil {
		return m.PayloadLen
	}
	return 0
}

func (m *Header) GetStream() bool {
	if m != nil {
		return m.Stream
	}
	return false
}

func (m *Header) GetEndpoint() string {
	if m != nil {
		return m.Endpoint
	}
	return ""
}

func (m *Header) GetEndpointError() string {
	if m != nil {
		return m.EndpointError
	}
	return ""
}

func (m *Header) GetClose() bool {
	if m != nil {
		return m.Close
	}
	return false
}

func init() {
	proto.RegisterType((*Header)(nil), "pdu.Header")
}

func init() { proto.RegisterFile("main.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 152 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0xca, 0x4d, 0xcc, 0xcc,
	0xd3, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2e, 0x48, 0x29, 0x55, 0x9a, 0xc1, 0xc8, 0xc5,
	0xe6, 0x91, 0x9a, 0x98, 0x92, 0x5a, 0x24, 0x24, 0xc7, 0xc5, 0x15, 0x90, 0x58, 0x99, 0x93, 0x9f,
	0x98, 0xe2, 0x93, 0x9a, 0x27, 0xc1, 0xa8, 0xc0, 0xa8, 0xc1, 0x1b, 0x84, 0x24, 0x22, 0x24, 0xc6,
	0xc5, 0x16, 0x5c, 0x52, 0x94, 0x9a, 0x98, 0x2b, 0xc1, 0xa4, 0xc0, 0xa8, 0xc1, 0x11, 0x04, 0xe5,
	0x09, 0x49, 0x71, 0x71, 0xb8, 0xe6, 0xa5, 0x14, 0xe4, 0x67, 0xe6, 0x95, 0x48, 0x30, 0x2b, 0x30,
	0x6a, 0x70, 0x06, 0xc1, 0xf9, 0x42, 0x2a, 0x5c, 0xbc, 0x30, 0xb6, 0x6b, 0x51, 0x51, 0x7e, 0x91,
	0x04, 0x0b, 0x58, 0x01, 0xaa, 0xa0, 0x90, 0x08, 0x17, 0xab, 0x73, 0x4e, 0x7e, 0x71, 0xaa, 0x04,
	0x2b, 0xd8, 0x60, 0x08, 0x27, 0x89, 0x0d, 0xec, 0x4c, 0x63, 0x40, 0x00, 0x00, 0x00, 0xff, 0xff,
	0x35, 0xb0, 0xcb, 0x3f, 0xb4, 0x00, 0x00, 0x00,
}
