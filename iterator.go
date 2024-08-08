package bitcask_db

import "bitcask-db/index"

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator
	db        *DB
	Options   IteratorOptions
}
