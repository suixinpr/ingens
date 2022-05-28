// entry structure

package bufnode

import (
	"encoding/binary"
	. "github/suixinpr/ingens/base"
	"unsafe"
)

// The structure of the index entry is as follows
//
// +------------------+-----------+-------------+---------+
// | indexEntryHeader |    key    |    value    | padding |
// +------------------+-----------+-------------+---------+
//
// indexEntryHeader holds the information of the index entry
// key represents k in kv
// value refers to the position of the next node (page id)

type (
	// non-leaf entry header
	indexEntryHeader struct {
		keySize OffsetNumber
	}

	// non-leaf entry
	IndexEntry []byte
)

const (
	// offset
	ieKeySizePos = OffsetNumber(unsafe.Offsetof(indexEntryHeader{}.keySize))
	ieHeaderSize = OffsetNumber(unsafe.Sizeof(indexEntryHeader{}))
	ieValueSize  = OffsetNumber(unsafe.Sizeof(PageNumber(0)))
)

// index entry
// key cannot be nil
func FormIndexEntry(key []byte, value PageNumber) IndexEntry {
	// get ks and ie
	ks := OffsetNumber(len((key)))
	ie := make([]byte, ieHeaderSize+ks+ieValueSize)

	// index entry header
	binary.BigEndian.PutUint16(ie[ieKeySizePos:], uint16(ks)) // keySize

	// key and value
	copy(ie[ieHeaderSize:], key)
	binary.BigEndian.PutUint64(ie[ieHeaderSize+ks:], uint64(value))

	return ie
}

func (ie IndexEntry) KeySize() OffsetNumber {
	return (*indexEntryHeader)(unsafe.Pointer(&ie[0])).keySize
}

func (ie IndexEntry) Key() []byte {
	return ie[ieHeaderSize : ieHeaderSize+ie.KeySize()]
}

func (ie IndexEntry) Value() PageNumber {
	pos := ieHeaderSize + ie.KeySize()
	return PageNumber(binary.BigEndian.Uint64(ie[pos:]))
}

func (ie IndexEntry) SetValue(value PageNumber) {
	pos := ieHeaderSize + ie.KeySize()
	binary.BigEndian.PutUint64(ie[pos:], uint64(value))
}

func (ie IndexEntry) Size() OffsetNumber {
	return ieHeaderSize + ie.KeySize() + ieValueSize
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

type (
	// leaf entry header
	dataEntryHeader struct {
		keySize   OffsetNumber
		valueSize OffsetNumber
		status    uint32

		tid      TransactionId
		rollback uint64
	}

	// leaf entry
	DataEntry []byte
)

const (
	// data entry
	deKeySizePos   = OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.keySize))
	deValueSizePos = OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.valueSize))
	deStatusPos    = OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.status))
	deTidPos       = OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.tid))
	deRollbackPos  = OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.rollback))
	deHeaderSize   = OffsetNumber(unsafe.Sizeof(dataEntryHeader{}))
)

// data entry
// key, value cannot be nil
func FormDataEntry(key, value []byte) DataEntry {
	ks := OffsetNumber(len(key))
	vs := OffsetNumber(len(value))
	de := make([]byte, deHeaderSize+ks+vs)

	// data entry header
	binary.BigEndian.PutUint16(de[deKeySizePos:], uint16(ks))
	binary.BigEndian.PutUint16(de[deValueSizePos:], uint16(vs))

	// key and value
	copy(de[deHeaderSize:deHeaderSize+ks], key)
	copy(de[deHeaderSize+ks:deHeaderSize+ks+vs], value)

	return de
}

func (de DataEntry) KeySize() OffsetNumber {
	return OffsetNumber(binary.BigEndian.Uint16(de[deKeySizePos:]))
}

func (de DataEntry) ValueSize() OffsetNumber {
	return OffsetNumber(binary.BigEndian.Uint16(de[deHeaderSize+de.KeySize():]))
}

func (de DataEntry) Tid() TransactionId {
	return TransactionId(binary.BigEndian.Uint64(de[deTidPos:]))
}

func (de DataEntry) Key() []byte {
	return de[deHeaderSize : deHeaderSize+de.KeySize()]
}

func (de DataEntry) Value() []byte {
	return de[deHeaderSize+de.KeySize():]
}

func (de DataEntry) Size() OffsetNumber {
	return deHeaderSize + de.KeySize() + de.ValueSize()
}
