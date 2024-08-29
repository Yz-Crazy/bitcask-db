package index

import (
	"bitcask-db/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

// b+ 索引
// https://github.com/etcd-io/bbolt
// go.etcd.io/bbolt

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree B+ 数索引
// 主要封装了 go.etcd.io/bbolt
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree 初始化 B+ 树索引
func NewBPlusTree(dirPath string) *BPlusTree {
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, nil)
	if err != nil {
		panic("failed to open bptree at startup")
	}
	// 创建对应的 bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

// Put 向索引中存储 key 对应的数据位置信息
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put key in bptree")
	}
	return true
}

// Get 根据 Key 取出对应的索引位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

// Delete 根据 key 删除对应的索引位置信息
func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete key in bptree")
	}
	return ok

}

// Size 索引中的数据量
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}

// Iterator 索引迭代器
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return nil
}
