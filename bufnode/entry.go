// entry structure

package bufnode

import (
	"encoding/binary"
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/memory"
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
		keySize base.OffsetNumber
	}

	// non-leaf entry
	IndexEntry []byte
)

const (
	// offset
	ieKeySizePos = base.OffsetNumber(unsafe.Offsetof(indexEntryHeader{}.keySize))
	ieHeaderSize = base.OffsetNumber(unsafe.Sizeof(indexEntryHeader{}))
	ieValueSize  = base.OffsetNumber(unsafe.Sizeof(base.PageNumber(0)))
)

// index entry
// key cannot be nil
func FormIndexEntry(key []byte, value base.PageNumber) IndexEntry {
	// get ks and ie
	ks := base.OffsetNumber(len((key)))
	ie := make([]byte, ieHeaderSize+ks+ieValueSize)

	// index entry header
	binary.BigEndian.PutUint16(ie[ieKeySizePos:], uint16(ks)) // keySize

	// key and value
	copy(ie[ieHeaderSize:], key)
	binary.BigEndian.PutUint64(ie[ieHeaderSize+ks:], uint64(value))

	return ie
}

func (ie IndexEntry) KeySize() base.OffsetNumber {
	return (*indexEntryHeader)(unsafe.Pointer(&ie[0])).keySize
}

func (ie IndexEntry) Key() []byte {
	return ie[ieHeaderSize : ieHeaderSize+ie.KeySize()]
}

func (ie IndexEntry) Value() base.PageNumber {
	pos := ieHeaderSize + ie.KeySize()
	return base.PageNumber(binary.BigEndian.Uint64(ie[pos:]))
}

func (ie IndexEntry) SetValue(value base.PageNumber) {
	pos := ieHeaderSize + ie.KeySize()
	binary.BigEndian.PutUint64(ie[pos:], uint64(value))
}

func (ie IndexEntry) Size() base.OffsetNumber {
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
		keySize   base.OffsetNumber
		valueSize base.OffsetNumber
		status    uint32

		tid      base.TransactionId
		rollback uint64
	}

	// leaf entry
	DataEntry []byte
)

const (
	// data entry
	deKeySizePos   = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.keySize))
	deValueSizePos = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.valueSize))
	deStatusPos    = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.status))
	deTidPos       = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.tid))
	deRollbackPos  = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.rollback))
	deHeaderSize   = base.OffsetNumber(unsafe.Sizeof(dataEntryHeader{}))
)

// data entry
// key, value cannot be nil
func FormDataEntry(memManager *memory.MemoryManager, key, value []byte) DataEntry {
	ks := base.OffsetNumber(len(key))
	vs := base.OffsetNumber(len(value))
	de := memManager.Alloc(uint32(deHeaderSize + ks + vs))

	// data entry header
	binary.BigEndian.PutUint16(de[deKeySizePos:], uint16(ks))
	binary.BigEndian.PutUint16(de[deValueSizePos:], uint16(vs))

	// key and value
	copy(de[deHeaderSize:deHeaderSize+ks], key)
	copy(de[deHeaderSize+ks:deHeaderSize+ks+vs], value)

	return de
}

func (de DataEntry) KeySize() base.OffsetNumber {
	return base.OffsetNumber(binary.BigEndian.Uint16(de[deKeySizePos:]))
}

func (de DataEntry) ValueSize() base.OffsetNumber {
	return base.OffsetNumber(binary.BigEndian.Uint16(de[deHeaderSize+de.KeySize():]))
}

func (de DataEntry) Tid() base.TransactionId {
	return base.TransactionId(binary.BigEndian.Uint64(de[deTidPos:]))
}

func (de DataEntry) Key() []byte {
	return de[deHeaderSize : deHeaderSize+de.KeySize()]
}

func (de DataEntry) Value() []byte {
	return de[deHeaderSize+de.KeySize():]
}

func (de DataEntry) Size() base.OffsetNumber {
	return deHeaderSize + de.KeySize() + de.ValueSize()
}
