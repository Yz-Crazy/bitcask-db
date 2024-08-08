package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("YZ-DB"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, n1)
	assert.Greater(t, n1, int64(5))
	t.Log(res1)

	// 第二种情况，值为空
	rec2 := &LogRecord{
		Key:  []byte("name1"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, n2)
	assert.Greater(t, n2, int64(5))
	t.Log(res2)

	// 对 Deleted 情况的测试
	rec3 := &LogRecord{
		Key:   []byte("name2"),
		Value: []byte("YZ-DB"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	assert.NotNil(t, n3)
	assert.Greater(t, n3, int64(5))
	t.Log(res3)
}

func TestDecodeLogRecord(t *testing.T) {
	// 前4个是crc校验值，下标为4 的是类型，最后两个是 key size 和value size
	headerBuf1 := []byte{78, 195, 109, 65, 0, 8, 10}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	//header length: 7,crc:1097712462,key size:4, value size:5
	//log_record_test.go:18: [78 195 109 65 0 8 10 110 97 109 101 89 90 45 68 66]
	t.Log(h1)
	t.Log(size1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(1097712462), h1.crc)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(5), h1.valueSize)

	//header length: 7,crc:1838227142,key size:5, value size:0
	//log_record_test.go:28: [198 30 145 109 0 10 0 110 97 109 101 49]
	headerBuf2 := []byte{198, 30, 145, 109, 0, 10, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	t.Log(h2)
	t.Log(size2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(1838227142), h2.crc)
	assert.Equal(t, uint32(5), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	//header length: 7,crc:679461690,key size:5, value size:5
	//log_record_test.go:39: [58 195 127 40 1 10 10 110 97 109 101 50 89 90 45 68 66]
	headerBuf3 := []byte{58, 195, 127, 40, 1, 10, 10}
	h3, size3 := decodeLogRecordHeader(headerBuf3)
	t.Log(h3)
	t.Log(size3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(679461690), h3.crc)
	assert.Equal(t, uint32(5), h3.keySize)
	assert.Equal(t, uint32(5), h3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("YZ-DB"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{78, 195, 109, 65, 0, 8, 10}
	crc1 := getLogRecordCRC(rec1, headerBuf1[4:])
	t.Log(crc1)
	assert.Equal(t, uint32(1097712462), crc1)

	rec2 := &LogRecord{
		Key:  []byte("name1"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{198, 30, 145, 109, 0, 10, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[4:])
	t.Log(crc2)
	assert.Equal(t, uint32(1838227142), crc2)

	rec3 := &LogRecord{
		Key:   []byte("name2"),
		Value: []byte("YZ-DB"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{58, 195, 127, 40, 1, 10, 10}
	crc3 := getLogRecordCRC(rec3, headerBuf3[4:])
	t.Log(crc3)
	assert.Equal(t, uint32(679461690), crc3)
}
