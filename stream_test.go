package streamrpc

import (
	"testing"
	"bytes"
	"io"
	"github.com/pkg/errors"
	"math"
)

func TestStreamEncoding(t *testing.T) {

	type testCase struct {
		in []byte
		csiz uint32
		rcsiz uint32
		err error
		msg string
	}

	cases := []testCase{
		{in: []byte{}, csiz: 1024, msg: "empty"},
		{in: []byte{0x23}, csiz: 1024, msg:"one byte"},
		{in: bytes.Repeat([]byte{0x23}, 1025), csiz: 512, msg: "multiple chunks"},
		{in: []byte{0x23, 0x24, 0x25}, csiz: 2, rcsiz: 1, err: ChunkSizeExceededError, msg:"excessive chunk size"},
	}

	for _, test := range cases {

		t.Logf("testing: %s", test.msg)

		src := bytes.NewBuffer(test.in)

		var buf bytes.Buffer
		err := writeStream(&buf, src, test.csiz)
		if err != nil {
			t.Errorf("writeStream should not fail writing to bytes.Buffer")
			continue
		}

		rcsiz := test.rcsiz
		if rcsiz == 0 {
			rcsiz = test.csiz
		}
		r := newStreamReader(&buf, rcsiz)
		var dest bytes.Buffer
		n, err := io.Copy(&dest, r)
		if err != nil && err != test.err {
			t.Errorf("unexpected error: expecting\n%#v\ngot\n%#v", test.err, err)
			continue
		}
		if err != nil {
			continue
		}
		if n != int64(len(test.in)) {
			t.Errorf("reader returned length %d != %d (expected)", n, len(test.in))
		}

		if bytes.Compare(test.in, dest.Bytes()) != 0 {
			t.Errorf("input bytes do not match output bytes.Expected:\t%#v\nGot: \t%#v", test.in, dest.Bytes())
		}

	}

}

type failingReader struct {
	Wrap io.Reader
	FailAt int
	FailWith error
	at int
}

func (r *failingReader) InducedFail() bool {
	return r.at >= r.FailAt
}

func (r *failingReader) Read(p []byte) (n int, err error) {
	if r.at >= r.FailAt {
		return 0, r.FailWith
	}
	n, err = r.Wrap.Read(p)
	r.at += n
	if r.at >= r.FailAt {
		return n, r.FailWith
	}
	return n, err
}


type failingWriter struct {
	Wrap io.Writer
	FailAt int
	FailWith error
	at int
}

func (w *failingWriter) InducedFail() bool {
	return w.at >= w.FailAt
}


func (w *failingWriter) Write(p []byte) (n int, err error) {
	if w.at >= w.FailAt {
		return 0, w.FailWith
	}
	n, err = w.Wrap.Write(p)
	w.at += n
	if w.at >= w.FailAt {
		return n, w.FailWith
	}
	return n, err
}

func TestStreamDecodingIOError(t *testing.T) {

	dataStr := []byte("this is a test string")
	var totalLen int
	{
		data := bytes.NewBuffer(dataStr)
		var stream bytes.Buffer
		writeStream(&stream, data, 2)
		totalLen = stream.Len()
	}

	for failAt := 0; failAt < totalLen; failAt++ {

		data := bytes.NewBuffer(dataStr)
		var stream bytes.Buffer
		writeStream(&stream, data, 2)

		e := errors.New("subtle error")

		fstream := failingReader{&stream, failAt, e, 0}

		dec := newStreamReader(&fstream, 10)
		var decBuf bytes.Buffer
		_, err := io.Copy(&decBuf, dec)
		if err != e {
			t.Errorf("streamReader should passs through io errors, but got %#v", err)
		}
		var buf [10]byte
		n, err := dec.Read(buf[:])
		if n != 0 {
			t.Errorf("subsequent calls to streamReader should return n = 0, but got %d", n)
		}
		if err != e {
			t.Errorf("subsequent calls should return the original io error, but got %#v", err)
		}
	}
}

func TestStreamEncodingSrcIOErrorForwarding(t *testing.T) {

	data := bytes.NewBufferString("this is a test")
	e := errors.New("123") // will be truncated to "12" because chunk size is 2
	fdata := failingReader{data, 3, e, 0}

	chunkSize := uint32(2)
	var out bytes.Buffer
	err := writeStream(&out, &fdata, chunkSize)
	if wrappedErr, ok := err.(*SourceStreamError); !ok || wrappedErr.StreamError != e {
		t.Errorf("writeStream should wrap reader io errors, but got: %#v", err)
	}

	dec := newStreamReader(&out, 2)
	var decBuf bytes.Buffer
	_, err = io.Copy(&decBuf, dec)
	if err == nil {
		t.Errorf("writeStream should have encoded an error, and streamReader should have returned it")
		return
	}
	if err.Error() != e.Error()[:chunkSize] {
		t.Errorf("error messages must be equal.\nexpected:\t%#v\ngot:\t%#v", e.Error(), err.Error())
	}

}


func TestStreamEncodingWriteIOError(t *testing.T) {

	dataStr := []byte("this is a test string that is longer than the chunk size")
	chunkSize := 2
	const chunkHeaderLen = 5 // uint32 + uint8
	chunks := int(math.Ceil(float64(len(dataStr)) / float64(chunkSize)))
	approxTotalLen := chunks * ( chunkSize + chunkHeaderLen) + (chunkHeaderLen + chunkSize) // data + error chunks

	for writeFailAt := 0; writeFailAt <= approxTotalLen; writeFailAt++ {
		for readFailAt := 0; readFailAt <= len(dataStr) + 1; readFailAt++ {
			writeErr := errors.New("write error")
			readErr := errors.New("read  error")
			data := bytes.NewBuffer(dataStr)
			fdata := failingReader{data, readFailAt, readErr, 0}
			var out bytes.Buffer
			fout := failingWriter{&out, writeFailAt, writeErr, 0}
			err := writeStream(&fout, &fdata, uint32(chunkSize))

			if fdata.InducedFail() && !fout.InducedFail() {
				// expect readError
				if wrappedErr, ok := err.(*SourceStreamError); !ok || wrappedErr.StreamError != readErr {
					t.Errorf("writeStream should wrap read errors, but got '%#v'", err)
				}
			} else {
				if err != writeErr {
					t.Errorf("writeStream should pass through write io errors, but got: %#v", err)
				}
			}
		}
	}

}