// entry structure

package bufpage

import (
	. "github/suixinpr/ingens/base"
	"unsafe"
)

const (
	// indexEntryHeaderSize
	indexEntryHeaderSize = OffsetNumber(unsafe.Sizeof(indexEntryHeader{}))

	// dataEntryHeaderSize
	dataEntryHeaderSize = OffsetNumber(unsafe.Sizeof(dataEntryHeader{}))
)

type (
	Entry interface {
		// entry 实际数据
		Data() []byte

		// entry 大小
		Size() OffsetNumber

		// entry 的key值
		Key() []byte
	}

	// The structure of the index entry is as follows
	//
	// +------------------+-----------+-------------+---------+
	// | indexEntryHeader |    key    |    value    | padding |
	// +------------------+-----------+-------------+---------+
	//
	// indexEntryHeader holds the information of the index entry
	// key represents k in kv
	// value refers to the position of the next node (page id)

	// non-leaf entry header
	indexEntryHeader struct {
		keySize OffsetNumber
	}

	// non-leaf entry
	IndexEntry struct {
		ptr unsafe.Pointer
	}

	// The structure of the data entry is as follows
	//
	// +-----------------+-----------+-------------+---------+
	// | dataEntryHeader |    key    |    value    | padding |
	// +-----------------+-----------+-------------+---------+
	//
	// dataEntryHeader holds the information of the data entry
	// key represents k in kv
	// value represents v in kv

	// leaf entry header
	dataEntryHeader struct {
		keySize   OffsetNumber
		valueSize OffsetNumber
		status    uint32

		tid      TransactionId
		rollback uint64
	}

	// leaf entry
	DataEntry struct {
		ptr unsafe.Pointer
	}
)

// index entry
// key cannot be nil
func FormIndexEntry(key []byte, value PageNumber) *IndexEntry {
	ks := OffsetNumber(len((key)))

	// 赋值key,value数据，生成IndexEntry
	data := make([]byte, indexEntryHeaderSize+ks+PageNumberSize)
	copy(data[indexEntryHeaderSize:indexEntryHeaderSize+ks], key)
	copy(data[indexEntryHeaderSize+ks:indexEntryHeaderSize+ks+PageNumberSize],
		unsafe.Slice((*byte)(unsafe.Pointer(&value)), PageNumberSize))
	ie := &IndexEntry{
		ptr: unsafe.Pointer(&data[0]),
	}

	// 初始化头部
	ieh := (*indexEntryHeader)(ie.ptr)
	ieh.keySize = ks

	return ie
}

func (ie *IndexEntry) Data() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ie.ptr))), ie.Size())
}

func (ie *IndexEntry) Key() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ie.ptr)+uintptr(indexEntryHeaderSize))), ie.keySize())
}

func (ie *IndexEntry) Value() PageNumber {
	return *(*PageNumber)(unsafe.Pointer(uintptr(ie.ptr) + uintptr(indexEntryHeaderSize) + uintptr(ie.keySize())))
}

func (ie *IndexEntry) Size() OffsetNumber {
	return indexEntryHeaderSize + ie.keySize() + PageNumberSize
}

func (ie *IndexEntry) keySize() OffsetNumber {
	return (*indexEntryHeader)(ie.ptr).keySize
}

func (ie *IndexEntry) SetValue(value PageNumber) {
	*(*PageNumber)(unsafe.Pointer(uintptr(ie.ptr) + uintptr(indexEntryHeaderSize) + uintptr(ie.keySize()))) = value
}

// data entry
// key, value cannot be nil
func FormDataEntry(key, value []byte) *DataEntry {
	ks := OffsetNumber(len(key))
	vs := OffsetNumber(len(value))

	// 赋值key,value数据，生成DataEntry
	data := make([]byte, dataEntryHeaderSize+ks+vs)
	copy(data[dataEntryHeaderSize:dataEntryHeaderSize+ks], key)
	copy(data[dataEntryHeaderSize+ks:dataEntryHeaderSize+ks+vs], value)
	de := &DataEntry{
		ptr: unsafe.Pointer(&data[0]),
	}

	// 初始化头部
	deh := (*dataEntryHeader)(de.ptr)
	deh.keySize = ks
	deh.valueSize = vs

	return de
}

func (de *DataEntry) Data() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(de.ptr))), de.Size())
}

func (de *DataEntry) Key() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(de.ptr)+uintptr(dataEntryHeaderSize))), de.keySize())
}

func (de *DataEntry) Value() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(de.ptr)+uintptr(dataEntryHeaderSize)+uintptr(de.keySize()))), de.valueSize())
}

func (de *DataEntry) Size() OffsetNumber {
	return dataEntryHeaderSize + de.keySize() + de.valueSize()
}

func (de *DataEntry) keySize() OffsetNumber {
	return (*dataEntryHeader)(unsafe.Pointer(uintptr(de.ptr))).keySize
}

func (de *DataEntry) valueSize() OffsetNumber {
	return (*dataEntryHeader)(unsafe.Pointer(uintptr(de.ptr))).valueSize
}
