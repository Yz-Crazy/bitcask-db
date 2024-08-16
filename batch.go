package bitcask_db

import (
	"bitcask-db/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// 原子性

const nonTransactionSeqNo = 0 // 非事务序列号

var txnFinKey = []byte("txn-fin")

// WriteBatch 原子批量写数据，保证原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

// NewWriteBatch 初始化 WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       opts,
		db:            db,
		mu:            new(sync.Mutex),
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 暂存
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 对不存在的数据直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 对暂存数据校验
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// 数据量超过最大限制，返回错误
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}
	// 加锁保证事务提交串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	// 开始写数据到数据文件中
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(
			&data.LogRecord{
				Key:   logRecordKeyWithSeqNo(record.Key, seqNo),
				Value: record.Value,
				Type:  record.Type,
			},
		)
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}
	// 事务提交成功标识
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(txnFinKey, seqNo),
		Type: data.LogRecordTxnFindShed,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引

	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}

	}

	// 清空暂存数据，方便下一次commit
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// key+seq number 编码
func logRecordKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 解析 LogRecord 的 key ，获取实际的key和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
