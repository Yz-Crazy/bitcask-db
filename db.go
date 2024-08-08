package bitcask_db

import (
	"bitcask-db/data"
	"bitcask-db/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 面向用户的操作接口

// DB 存储数据结构体
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // 只能用于加载索引的时候使用
	activeFile *data.DataFile            // 当前活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Index               // 内存索引
}

// Open 打开 bitcask 存储引擎实例

func Open(options Options) (*DB, error) {
	// 校验用户配置
	err := checkOptions(options)
	if err != nil {
		return nil, err
	}
	// 对用户传递过来的目录进行校验
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 初始化 db 结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	// 从数据文件中加载索引
	if err := db.loadIndexFromFiles(); err != nil {
		return nil, err
	}

	// 起动删除任务

	return db, nil

}

// Put 写入 key value 数据，key 不能为空
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体

	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 追加写入到当前活跃数据文件当中
	pos, err := db.appendLogRecord(log_record)
	if err != nil {
		return err
	}
	// 拿到内存信息之后，更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	// 从内存数据结构中取出 key 对应的索引信息
	logrecordPos := db.index.Get(key)
	// 如果 key 不存在内存索引中，那么这个key就不存在
	if logrecordPos == nil {
		return nil, ErrKeyNotFound
	}
	// 根据文件 ID 找到对应的数据文件

	var dataFile *data.DataFile
	// 如果是在当前活跃文件就在当前活跃文件去找
	// 不在当前文件，就去旧文件去找
	if db.activeFile.FileId == logrecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logrecordPos.Fid]
	}
	// 文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移量来读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logrecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// Delete 根据 key 删除数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 从内存索引中查找key是否存在
	dataFilePos := db.index.Get(key)
	if dataFilePos == nil {
		return ErrDataFileNotFound
	}

	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}

	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// 从内存索引中删除对应的 key

	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// appendLogRecord 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	//defer db.mu.Unlock()
	//db.mu.RLock()
	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果为空，初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经达到了活跃文件的阀值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// 先持久化数据文件，保证已有文件能持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 持久化完成之后把当前活跃文件转换为旧的活跃文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 数据写入

	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	// 检查是否需要对数据进行持久化
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构造内存存储信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOffset}
	//db.mu.Unlock()
	return pos, nil
}

// setActiveDataFile 设备当前活跃文件
// 在访问此方法钱必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	// 当前活跃文件不为空，新的活跃文件ID是当前文件ID+1
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// loadDataFiles 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 从目录中遍历以 data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 对文件进行分割，000001.data
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 对文件 ID 进行排序，从小到大依次加载
	sort.Ints(fileIds)
	// 遍历每个文件ID，打开对应的数据文件

	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			// 最后一个，id是最大的，说明是当前活跃文件
			db.activeFile = dataFile
		} else {
			// 说明是旧数据文件
			db.olderFiles[uint32(fileId)] = dataFile
		}
	}
	db.fileIds = fileIds
	return nil
}

// loadIndexFromFiles 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromFiles() error {
	// db.fileIds ===0 数据库为空
	if len(db.fileIds) == 0 {
		return nil
	}
	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF { // 文件读完了
					break
				}
				return err
			}
			// 把读出来的数据保存到内存索引当中
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			// 检查数据类型，如果存在就插入，如果被删除就从内存中删除
			var ok bool
			if logRecord.Type == data.LogRecordDeleted {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的WriteOffset
		if i == len(db.fileIds)-1 {
			dataFile.WriteOffset = offset
		}
	}

	return nil
}

// checkOptions 校验用户配置
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")

	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}