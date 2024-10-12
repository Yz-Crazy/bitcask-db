package bitcask_db

import (
	"bitcask-db/data"
	"bitcask-db/fio"
	"bitcask-db/index"
	"bitcask-db/utils"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 面向用户的操作接口

const (
	seqNokey     = "seq.no"
	fileLockName = "flock"
)

// DB 存储数据结构体
type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIds         []int                     // 只能用于加载索引的时候使用
	activeFile      *data.DataFile            // 当前活跃数据文件，可以用于写入
	olderFiles      map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index           index.Index               // 内存索引
	seqNo           uint64                    // 事务序列号，全局递增
	isMerging       bool                      // 是否正在merge
	seqNoFileExists bool                      // 存储事务序列号的文件是否存在
	isInitial       bool                      // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock              // 文件锁，保证多进程之间的互斥
	bytesWrite      uint                      // 记录写入多少字节数
	reclaimSize     int64                     // 表示有多少数据是无效的
}

type Stat struct {
	KeyNum          uint  // key 的总数量
	DataFileNum     uint  // 磁盘上数据文件的总数量
	ReclaimableSize int64 // 可以进行 merge 回收的数据量，以字节为单位
	DiskSize        int64 // 数据目录占用磁盘空间大小
}

// Open 打开 bitcask 存储引擎实例

func Open(options Options) (*DB, error) {
	// 校验用户配置
	err := checkOptions(options)
	if err != nil {
		return nil, err
	}
	var isInitial bool
	// 对用户传递过来的目录进行校验
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 判断当前文件是否被其他进程持有
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}
	// 初始化 db 结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}
	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// 加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	// B+ 树索引不需要从数据文件中加载索引
	if options.IndexType != BPlusTree {
		// 加载 Hint 文件中的索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		// 重置 IO
		if db.options.MMapAtStartup {
			if err := db.resetIOType(); err != nil {
				return nil, err
			}
		}
	}

	// 取出当前事务序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
	}

	return db, nil

}

// Put 写入 key value 数据，key 不能为空
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体

	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeqNo(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 追加写入到当前活跃数据文件当中
	pos, err := db.appendLogRecordWitchLock(logRecord)
	if err != nil {
		return err
	}
	// 拿到内存信息之后，更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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
	return db.getValueByPosition(logrecordPos)

}

// Delete 根据 key 删除数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 从内存索引中查找key是否存在
	dataFilePos := db.index.Get(key)
	if dataFilePos == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	pos, err := db.appendLogRecordWitchLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	// 从内存索引中删除对应的 key

	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the director, %v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNokey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的活跃文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}
	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize, // TODO 等待补全
	}
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// 获取所有的数据，并执行用户指定的操作,函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	// 使用完如果不关闭将会阻塞
	defer db.index.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPosition 根据索引信息获取对应的value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	// 如果是在当前活跃文件就在当前活跃文件去找
	// 不在当前文件，就去旧文件去找
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	// 文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移量来读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWitchLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

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

	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}
	// 构造内存存储信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOffset, Size: uint32(size)}
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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
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
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId), ioType)
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
func (db *DB) loadIndexFromDataFiles() error {
	// db.fileIds ===0 数据库为空
	if len(db.fileIds) == 0 {
		return nil
	}
	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid

	}

	updateIndex := func(key []byte, typ data.LogRecordType, logRecordPos *data.LogRecordPos) {
		// 检查数据类型，如果存在就插入，如果被删除就从内存中删除
		var oldPos *data.LogRecordPos

		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(logRecordPos.Size)
		} else {
			oldPos = db.index.Put(key, logRecordPos)
		}

		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	var currentSeqNo uint64 = nonTransactionSeqNo
	transactionRecords := make(map[uint64][]*data.TransactionRecord)

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果比最近未参与 merge 的文件 ID 还小，就说明已经从 Hint 文件中加载过了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

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
			pos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			// 解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, pos)
			} else {
				// 事务完成，对应的 seq no的数据可以更新到内存当中

				if logRecord.Type == data.LogRecordTxnFindShed {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					// 暂存
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    pos,
					})
				}

			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			//  递增 offset ，下一次从新的位置开始读取
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的WriteOffset
		if i == len(db.fileIds)-1 {
			dataFile.WriteOffset = offset
		}
	}
	// 更新事务序列号
	db.seqNo = currentSeqNo
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

	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("database data file merge rotio must be between 0 and 1")
	}

	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	// 加载完后删除文件防止 seqNo 一直追加
	return os.Remove(fileName)
}

// resetIOType 将数据文件的 IO 类重置为标准文件 IO
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil

}
