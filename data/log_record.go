package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置

type LogRecordPos struct {
	Fid    uint32 // 文件ID，表示将文件存储在了哪个文件当中
	Offset int64  // 偏移，表示将数据存储在数据文件中的哪个位置
}

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// crc type keySize valueSize
// 4+1+5+5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似于日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 长度
	valueSize  uint32        // value 长度
}

// EncodeLogRecord 对 LogRecord 进行编码操作，返回字节数据及长度
// crc 校验 4字节
// type 类型 1字节
// key size 变长（最大5字节）
// value size 变长 （最大5字节）
// key 变长
// value 变长

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	if logRecord == nil {
		return nil, -1
	}
	header := make([]byte, maxLogRecordHeaderSize)

	// 从第5个字节开始写，
	header[4] = logRecord.Type
	var index = 5
	// 5 字节之后，存储的是 key size，value size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	var size = index + len(logRecord.Key) + len(logRecord.Value)

	encBytes := make([]byte, size)
	// 将 header 部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	// 将 key value 数据拷贝到字节数组中

	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个 LogRecord 的数据进行 CRC 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	//fmt.Printf("header length: %d,crc:%d,key size:%d, value size:%d\n", len(header[:index]), crc, len(logRecord.Key), len(logRecord.Value))
	return encBytes, int64(size)
}

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n // 实际的header长度
	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
