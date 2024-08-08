package index

import (
	"bitcask-db/data"
	"github.com/google/btree"
	"sync"
)

// BTree 索引，主要封装了 google 的btree 库

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex // 因为 google btree 并发不安全，所以这里要加一个锁
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), // 这里可以由用湖自己选择或者自己测试一个适合的大小
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	defer bt.lock.Unlock()

	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	// 这里需要把 Item 强转成 Item 指针，然后取 pos
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	defer bt.lock.Unlock()
	it := &Item{key: key}
	bt.lock.Lock()
	btreeItem := bt.tree.Delete(it)
	if btreeItem == nil {
		return false
	}
	return true
}
