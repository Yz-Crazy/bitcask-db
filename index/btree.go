package index

import (
	"bitcask-db/data"
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {

	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
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

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {

	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	if bt.tree == nil {
		return nil
	}
	return newBtreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

// Btree 索引迭代器
type btreeIterator struct {
	currIndex int     // 当前遍历下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key + 位置索引信息
}

// newBtreeIterator 实例化 bterrIterator
func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}

	// 排序
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bit *btreeIterator) Rewind() {
	bit.currIndex = 0
}

func (bit *btreeIterator) Seek(key []byte) {
	if bit.reverse {
		bit.currIndex = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].key, key) <= 0
		})
	} else {
		bit.currIndex = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].key, key) >= 0
		})
	}

}

func (bit *btreeIterator) Next() {
	bit.currIndex += 1
}
func (bit *btreeIterator) Valid() bool {

	return bit.currIndex < len(bit.values)
}
func (bit *btreeIterator) Key() []byte {

	return bit.values[bit.currIndex].key
}
func (bit *btreeIterator) Value() *data.LogRecordPos {
	return bit.values[bit.currIndex].pos
}
func (bit *btreeIterator) Close() {
	bit.values = nil
}
