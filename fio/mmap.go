package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO、内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化 MMap IO
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// Read 从文件的给定位置读取对应的数据
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {

	return mmap.readerAt.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// Sync 从内存缓冲区的数据持久化到磁盘中
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// Close 结束读写操作
func (mmap *MMap) Close() error { return mmap.readerAt.Close() }

// Size 获取到文件大小
func (mmap *MMap) Size() (int64, error) { return int64(mmap.readerAt.Len()), nil }
