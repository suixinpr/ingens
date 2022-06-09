package nodes

import (
	"encoding/binary"
	"github/suixinpr/ingens/base"
	"unsafe"
)

// The structure of the page is as follows
//
// +----------------------------------+-----------+-----------+
// |            pageHeader            | entryPtr1 | entryPtr2 |
// +-----------+----------------------+-----------+-----------+
// | entryPtr3 |                                              |
// +-----------+----------------------------------------------+
// |                                                          |
// +--------+------------+------------+------------+----------+
// |        |   entry3   |   entry2   |   entry1   | checksum |
// +--------+------------+------------+------------+----------+
//
// indexEntryHeader holds the information of the index entry
// key represents k in kv
// value refers to the position of a child page (page id)

type (
	// page header
	pageHeader struct {
		pageId base.PageNumber

		lower base.OffsetNumber
		upper base.OffsetNumber
		level uint16

		left  base.PageNumber
		right base.PageNumber
	}

	// page
	Page []byte
)

const (
	// member offset in page entry
	pageIdPos = base.OffsetNumber(unsafe.Offsetof(pageHeader{}.pageId))
	lowerPos  = base.OffsetNumber(unsafe.Offsetof(pageHeader{}.lower))
	upperPos  = base.OffsetNumber(unsafe.Offsetof(pageHeader{}.upper))
	levelPos  = base.OffsetNumber(unsafe.Offsetof(pageHeader{}.level))
	leftPos   = base.OffsetNumber(unsafe.Offsetof(pageHeader{}.left))
	rightPos  = base.OffsetNumber(unsafe.Offsetof(pageHeader{}.right))

	// page header Size
	pageHeaderSize = base.OffsetNumber(unsafe.Sizeof(pageHeader{}))

	// offset size
	EntryPtrSize = base.OffsetNumber(unsafe.Sizeof(base.OffsetNumber(0)))
)

// 将off从页面内的位置转换为数组的形式
func offsetToArray(off base.OffsetNumber) base.OffsetNumber {
	return base.OffsetNumber((off - pageHeaderSize) / EntryPtrSize)
}

// 将off从数组的形式转换为页面内的位置
func arrayToOffset(off base.OffsetNumber) base.OffsetNumber {
	return pageHeaderSize + off*EntryPtrSize
}

// 获取entryPtr
func (p Page) getEntryPtr(off base.OffsetNumber) base.OffsetNumber {
	return base.OffsetNumber(binary.BigEndian.Uint16(p[off:]))
}

// 根据entryPtr的off获取entry
// 如果是叶子节点则返回DataEntry, 否则返回IndexEntry
func (p Page) getIndexEntry(off base.OffsetNumber) IndexEntry {
	entryPtr := p.getEntryPtr(off)
	return []byte(p[entryPtr:])
}

func (p Page) getDataEntry(off base.OffsetNumber) DataEntry {
	entryPtr := p.getEntryPtr(off)
	return []byte(p[entryPtr:])
}
