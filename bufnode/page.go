package bufnode

import (
	"encoding/binary"
	. "github/suixinpr/ingens/base"
	"io"
	"os"
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
		pageId PageNumber

		lower OffsetNumber
		upper OffsetNumber
		flag  uint16
		level uint16

		left  PageNumber
		right PageNumber
	}

	// page
	Page []byte
)

const (
	// member offset in page entry
	pageIdPos = OffsetNumber(unsafe.Offsetof(pageHeader{}.pageId))
	lowerPos  = OffsetNumber(unsafe.Offsetof(pageHeader{}.lower))
	upperPos  = OffsetNumber(unsafe.Offsetof(pageHeader{}.upper))
	flagPos   = OffsetNumber(unsafe.Offsetof(pageHeader{}.flag))
	levelPos  = OffsetNumber(unsafe.Offsetof(pageHeader{}.level))
	leftPos   = OffsetNumber(unsafe.Offsetof(pageHeader{}.left))
	rightPos  = OffsetNumber(unsafe.Offsetof(pageHeader{}.right))

	// page header Size
	pageHeaderSize = OffsetNumber(unsafe.Sizeof(pageHeader{}))

	// offset size
	EntryPtrSize = OffsetNumber(unsafe.Sizeof(OffsetNumber(0)))
)

// 将off从页面内的位置转换为数组的形式
func offsetToArray(off OffsetNumber) OffsetNumber {
	return OffsetNumber((off - pageHeaderSize) / EntryPtrSize)
}

// 将off从数组的形式转换为页面内的位置
func arrayToOffset(off OffsetNumber) OffsetNumber {
	return pageHeaderSize + off*EntryPtrSize
}

func (p Page) getEntryPtr(off OffsetNumber) OffsetNumber {
	return OffsetNumber(binary.BigEndian.Uint16(p[off:]))
}

// 根据entryPtr的off获取entry
// 如果是叶子节点则返回DataEntry, 否则返回IndexEntry
func (p Page) getIndexEntry(off OffsetNumber) IndexEntry {
	entryPtr := p.getEntryPtr(off)
	return *(*IndexEntry)(unsafe.Pointer(&p[entryPtr]))
}

func (p Page) getDataEntry(off OffsetNumber) DataEntry {
	entryPtr := p.getEntryPtr(off)
	return *(*DataEntry)(unsafe.Pointer(&p[entryPtr]))
}

// io 操作，从文件读取页面
func (p Page) readFile(file *os.File, pageId PageNumber) error {
	// 读取数据
	off := int64(pageId) * int64(PageSize)
	n, err := file.ReadAt(p, off)

	// 读取失败
	if err != nil {
		return err
	}

	// 读取数据长度不对
	if n != PageSize {
		return io.ErrUnexpectedEOF
	}

	return nil
}

// io 操作，将页面写入文件
func (p Page) writeFile(file *os.File, pageId PageNumber) error {
	// 写入数据
	off := int64(pageId) * int64(PageSize)
	n, err := file.WriteAt(p, off)

	// 写入失败
	if err != nil {
		return err
	}

	// 写入数据长度不对
	if n != PageSize {
		return io.ErrShortWrite
	}

	return nil
}
