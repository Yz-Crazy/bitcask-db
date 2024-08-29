package index

import (
	"bitcask-db/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree_test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree_test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("aac"))
	t.Log(pos1)
	assert.NotNil(t, pos1)
	assert.Equal(t, &data.LogRecordPos{Fid: 123, Offset: 999}, pos1)
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 9884, Offset: 1234})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)
	assert.Equal(t, &data.LogRecordPos{Fid: 9884, Offset: 1234}, pos2)
	//tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})

}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree_test")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()

	tree := NewBPlusTree(path)
	res1 := tree.Delete([]byte("not exist"))
	assert.False(t, res1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	res2 := tree.Delete([]byte("aac"))
	assert.True(t, res2)
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)

}
