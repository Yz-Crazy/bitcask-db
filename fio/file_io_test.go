package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	fp, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fp)
	defer destoryFile(filepath.Join("/tmp", "a.data"))
}

func TestFileIO_Write(t *testing.T) {
	fp, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fp)

	n, err := fp.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fp.Write([]byte("hello world"))
	assert.Equal(t, 11, n)
	assert.Nil(t, err)
	destoryFile(filepath.Join("/tmp", "a.data"))
}

func TestFileIO_Read(t *testing.T) {
	fp, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fp)

	_, err = fp.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fp.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fp.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)
	assert.Nil(t, err)

	b2 := make([]byte, 5)
	n, err = fp.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
	assert.Nil(t, err)

	destoryFile(filepath.Join("/tmp", "a.data"))
}

func TestFileIO_Sync(t *testing.T) {
	fp, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fp)
	err = fp.Sync()
	assert.Nil(t, err)
	destoryFile(filepath.Join("/tmp", "a.data"))
}

func TestFileIO_Close(t *testing.T) {
	fp, err := NewFileIOManager(filepath.Join("/tmp", "0001.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fp)
	err = fp.Close()
	assert.Nil(t, err)
	destoryFile(filepath.Join("/tmp", "0001.data"))
}
