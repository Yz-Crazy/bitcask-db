package index

import (
	"bitcask-db/data"
	"bytes"
	"github.com/google/btree"
)

// Index 抽象索引接口，后续如果想要接入其它的数据结构，则直接实现这个接口即可
type Index interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 Key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1
	// ART 自适应基数树索引
	ART
)

// NewIndex

func NewIndexer(typ IndexType) Index {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// TODO:
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}
