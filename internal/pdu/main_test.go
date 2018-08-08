package pdu

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorOnNonEqualVersions(t *testing.T) {
	m := newMagic(CurrentVersion - 1)
	var b bytes.Buffer
	b.Write(m)
	err := ReadMagic(&b)
	require.Error(t, err)
	verr, ok := err.(*VersionNumberError)
	require.True(t, ok)
	exp := &VersionNumberError{expected: CurrentVersion, received: CurrentVersion - 1}
	require.Equal(t, exp, verr)
}

func TestErrorOnWrongMagic(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("SSH-2.0-OpenSSH_7.5 FreeBSD-20170903")
	err := ReadMagic(&b)
	require.Equal(t, InvalidMagic, err)
}

func TestWriteReadWorks(t *testing.T) {
	var b bytes.Buffer
	require.NoError(t, WriteMagic(&b))
	require.NoError(t, ReadMagic(&b))
	assert.Equal(t, 0, b.Len())
}
