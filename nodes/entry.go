// entry structure

package nodes

import (
	"encoding/binary"
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/manager/memory"
	"unsafe"
)

// The structure of the index entry is as follows
//
// +------------------+-----------+-------------+
// | indexEntryHeader |    key    |    value    |
// +------------------+-----------+-------------+
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
	// member offset in index entry header
	ieKeySizePos = base.OffsetNumber(unsafe.Offsetof(indexEntryHeader{}.keySize))

	// index entry header size
	ieHeaderSize = base.OffsetNumber(unsafe.Sizeof(indexEntryHeader{}))

	// index entry value size
	ieValueSize = base.OffsetNumber(unsafe.Sizeof(base.PageNumber(0)))
)

// index entry
// key cannot be nil
func NewIndexEntry(mmgr *memory.MemoryManager, key []byte, value base.PageNumber) IndexEntry {
	// get ks and ie
	ks := base.OffsetNumber(len((key)))
	ts := ieHeaderSize + ks + ieValueSize
	ie := mmgr.Alloc(uint32(ts))

	// index entry header
	binary.BigEndian.PutUint16(ie[ieKeySizePos:], uint16(ks)) // keySize

	// key and value
	copy(ie[ieHeaderSize:], key)
	binary.BigEndian.PutUint64(ie[ieHeaderSize+ks:], uint64(value))

	return ie
}

func (ie IndexEntry) KeySize() base.OffsetNumber {
	return base.OffsetNumber(binary.BigEndian.Uint16(ie))
}

func (ie IndexEntry) Key() []byte {
	return ie[ieHeaderSize : ieHeaderSize+ie.KeySize()]
}

func (ie IndexEntry) Value() base.PageNumber {
	return base.PageNumber(binary.BigEndian.Uint64(ie[ieHeaderSize:]))
}

func (ie IndexEntry) Size() base.OffsetNumber {
	return ieHeaderSize + ie.KeySize() + ieValueSize
}

// update

func (ie IndexEntry) UpdateValue(value base.PageNumber) {
	binary.BigEndian.PutUint64(ie[ieHeaderSize+ie.KeySize():], uint64(value))
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
		totalSize base.OffsetNumber

		status uint8
		tid    base.TransactionId
		prev   base.UndoRecordPtr
	}

	// leaf entry
	DataEntry []byte
)

const (
	// member offset in data entry header
	deKeySizePos    = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.keySize))
	deValueSizePos  = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.valueSize))
	deTotalSizePos  = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.totalSize))
	deStatusPos     = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.status))
	deTidPos        = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.tid))
	deUndoRecPtrPos = base.OffsetNumber(unsafe.Offsetof(dataEntryHeader{}.prev))

	// index entry header size
	deHeaderSize = base.OffsetNumber(unsafe.Sizeof(dataEntryHeader{}))

	// status
	dead  uint8 = 0x01
	null        = 0x02
	large       = 0x03
)

// data entry
// key, value cannot be nil
func NewDataEntry(mmgr *memory.MemoryManager, tid base.TransactionId, key, value []byte) DataEntry {
	ks := base.OffsetNumber(len(key))
	vs := base.OffsetNumber(len(value))
	ts := deHeaderSize + ks + vs
	de := mmgr.Alloc(uint32(ts))

	// data entry header
	binary.BigEndian.PutUint16(de[deKeySizePos:], uint16(ks))
	binary.BigEndian.PutUint16(de[deValueSizePos:], uint16(vs))
	binary.BigEndian.PutUint16(de[deTotalSizePos:], uint16(ts))
	de[deStatusPos] = 0
	binary.BigEndian.PutUint64(de[deTidPos:], uint64(tid))

	// key and value
	copy(de[deHeaderSize:deHeaderSize+ks], key)
	copy(de[deHeaderSize+ks:deHeaderSize+ks+vs], value)

	return de
}

func (de DataEntry) KeySize() base.OffsetNumber {
	return base.OffsetNumber(binary.BigEndian.Uint16(de[deKeySizePos:]))
}

func (de DataEntry) ValueSize() base.OffsetNumber {
	return base.OffsetNumber(binary.BigEndian.Uint16(de[deValueSizePos:]))
}

func (de DataEntry) Tid() base.TransactionId {
	return base.TransactionId(binary.BigEndian.Uint64(de[deTidPos:]))
}

func (de DataEntry) Key() []byte {
	return de[deHeaderSize : deHeaderSize+de.KeySize()]
}

func (de DataEntry) Value() []byte {
	return de[deHeaderSize+de.KeySize() : deHeaderSize+de.KeySize()+de.ValueSize()]
}

func (de DataEntry) Size() base.OffsetNumber {
	return deHeaderSize + de.KeySize() + de.ValueSize()
}

func (de DataEntry) TotalSize() base.OffsetNumber {
	return base.OffsetNumber(binary.BigEndian.Uint16(de[deTotalSizePos:]))
}

// update

func (de DataEntry) UpdateTid(tid base.TransactionId) {
	binary.BigEndian.PutUint64(de[deTidPos:], uint64(tid))
}

func (de DataEntry) UpdateUndoRecordPtr(undoRecPtr base.UndoRecordPtr) {
	binary.BigEndian.PutUint64(de[deUndoRecPtrPos:], uint64(undoRecPtr))
}

// status

func (de DataEntry) IsDead() bool {
	return de[deStatusPos]&dead == dead
}

func (de DataEntry) IsNull() bool {
	return de[deStatusPos]&null == null
}

func (de DataEntry) MarkDead() {
	de[deStatusPos] |= dead
}

func (de DataEntry) MarkNull() {
	de[deStatusPos] |= null
}
