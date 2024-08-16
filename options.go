package bitcask_db

import "os"

type Options struct {
	// 数据库目录
	DirPath string
	// 数据文件的大小
	DataFileSize int64

	// 每次写入完毕执行持久化
	SyncWrite bool

	// 索引类型
	IndexType IndexerType
}

type IndexerType = int8

const (
	//BTree 索引
	BTree IndexerType = iota + 1
	// ART 自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, //256M
	SyncWrite:    false,
	IndexType:    BTree,
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 边你前缀为指定值的key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 是正向
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

// WriteBatchOptions 批量读写配置
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint
	// 提交事务时是否 Sync 持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
