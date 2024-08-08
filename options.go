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
	SyncWrite:    true,
	IndexType:    BTree,
}
