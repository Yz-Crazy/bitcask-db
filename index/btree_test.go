package index

import (
	"bitcask-db/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{2, 12})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.Nil(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{1, 3})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)

}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.Nil(t, res1)
	res2, ok1 := bt.Delete(nil)
	assert.True(t, ok1)
	assert.Equal(t, res2.Fid, uint32(1))
	assert.Equal(t, res2.Offset, int64(100))

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{1, 3})
	assert.Nil(t, res3)
	res4, ok2 := bt.Delete([]byte("a"))
	assert.True(t, ok2)
	assert.Equal(t, res4.Fid, uint32(1))
	assert.Equal(t, res4.Offset, int64(3))
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	// Btree 为空
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// Btree 有数据
	bt1.Put([]byte("ccde"), &data.LogRecordPos{1, 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	t.Log(iter2.Key())
	t.Log(iter2.Value())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())

	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 有多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{1, 10})
	bt1.Put([]byte("eede"), &data.LogRecordPos{1, 10})
	bt1.Put([]byte("bbcd"), &data.LogRecordPos{1, 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log(string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		//t.Log(string(iter4.Key()))
		assert.NotNil(t, iter4.Key())
	}

	// seek 测试

	iter5 := bt1.Iterator(false)

	//t.Log(string(iter5.Key()))
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	iter6 := bt1.Iterator(true)
	iter6.Seek([]byte("bb"))
	t.Log(string(iter6.Key()))
	for iter6.Rewind(); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}

}
