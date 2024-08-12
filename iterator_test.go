package bitcask_db

import (
	"bitcask-db/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)
}

func TestDB_Iterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("randomA"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("randomB"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("randomC"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("randomD"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("randomE"), utils.RandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter1)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
		t.Log("Key = ", string(iter1.Key()))
	}
	iter1.Rewind()
	for iter1.Seek([]byte("randomB")); iter1.Valid(); iter1.Next() {
		t.Log("Key1 = ", string(iter1.Key()))
	}

	// 反向迭代
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	assert.NotNil(t, iter2)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
		//t.Log("Key2 = ", string(iter2.Key()))
	}

	iter2.Rewind()
	for iter2.Seek([]byte("randomD")); iter2.Valid(); iter2.Next() {
		t.Log("Key2 = ", string(iter2.Key()))
	}

	// 指定了prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("randomB")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("Key = ", string(iter3.Key()))

	}

}
