package bufpage

import (
	"encoding/binary"
	"errors"
	. "github/suixinpr/ingens/base"
	"io"
	"os"
	"unsafe"
)

var (
	// errInvalidEntry Nil entry cannot be inserted into the page
	errInvalidEntry = errors.New("Nil entry cannot be inserted into the page.")

	// errLargeEntry Entry is too large to be inserted into the page
	errLargeEntry = errors.New("Entry is too large to be inserted into the page.")

	// errRepeatedEntry Entry already exists and cannot be inserted repeatedly
	errRepeatedEntry = errors.New("Entry already exists and cannot be inserted repeatedly.")

	// errNotBranch The node is not a non-leaf node
	errNotBranch = errors.New("The node is not a non-leaf node")

	// errRedirected
	errRedirected = errors.New("src cannot be redirected to itself")

	// errNotFound
	errNotFound = errors.New("Not Found")

	// errEmptyPage
	errEmptyPage = errors.New("The page is empty")

	// errNilPage
	errNilPage = errors.New("The page is nil")

	// errSmallPage
	errSmallPage = errors.New("The page is small")
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
	// pageHeader Size
	pageHeaderSize = OffsetNumber(unsafe.Sizeof(pageHeader{}))

	// offset size
	EntryPtrSize = OffsetNumber(unsafe.Sizeof(OffsetNumber(0)))

	// position
	pageIdPos = OffsetNumber(unsafe.Offsetof(pageHeader{}.pageId))
	lowerPos  = OffsetNumber(unsafe.Offsetof(pageHeader{}.lower))
	upperPos  = OffsetNumber(unsafe.Offsetof(pageHeader{}.upper))
	flagPos   = OffsetNumber(unsafe.Offsetof(pageHeader{}.flag))
	levelPos  = OffsetNumber(unsafe.Offsetof(pageHeader{}.level))
	leftPos   = OffsetNumber(unsafe.Offsetof(pageHeader{}.left))
	rightPos  = OffsetNumber(unsafe.Offsetof(pageHeader{}.right))
)

/*
// 获取某一层的空页面
func EmptryPage(pageId PageNumber, level uint16) Page {
	p := make([]byte, PageSize)

	// page header
	binary.BigEndian.PutUint64(p[pageIdPos:], uint64(pageId))
	binary.BigEndian.PutUint16(p[flagPos:], 0)
	binary.BigEndian.PutUint16(p[levelPos:], uint16(level))
	binary.BigEndian.PutUint16(p[lowerPos:], uint16(pageHeaderSize))
	binary.BigEndian.PutUint16(p[upperPos:], uint16(PageDataUpper))

	return p
}*/

/*
// 获取页面中的信息

// 获取该页面的id
func (p Page) GetPageId() PageNumber {
	return PageNumber(binary.BigEndian.Uint64(p[pageIdPos:]))
}

// 获取页面中第一个entryPtr的位置
func (p Page) GetStartEntryPtrPos() OffsetNumber {
	return pageHeaderSize
}

// 获取页面中最后一个entryPtr的位置
func (p Page) GetEndEntryPtrPos() OffsetNumber {
	return OffsetNumber(binary.BigEndian.Uint16(p[lowerPos:]))
}


// 获取层数
func (p Page) GetLevel() uint16 {
	return binary.BigEndian.Uint16(p[levelPos:])
}

// 获取右兄弟页
func (p Page) GetRight() PageNumber {
	return PageNumber(binary.BigEndian.Uint64(p[rightPos:]))
}

// 获取左兄弟页
func (p Page) GetLeft() PageNumber {
	return PageNumber(binary.BigEndian.Uint64(p[leftPos:]))
}


func (p Page) GetKey(off OffsetNumber) []byte {
	if p.IsLeaf() {
		e := p.GetDataEntry(off)
		return e.Key()
	} else {
		e := p.GetIndexEntry(off)
		return e.Key()
	}
}*/

/*
func (p Page) GetHighKey() []byte {
	off := p.GetEndEntryPtrPos() - EntryPtrSize
	return p.GetKey(off)
}*/

func (p Page) GetEntryPtr(off OffsetNumber) OffsetNumber {
	return OffsetNumber(binary.BigEndian.Uint16(p[off:]))
}

// 根据entryPtr的off获取entry
// 如果是叶子节点则返回DataEntry, 否则返回IndexEntry
func (p Page) GetIndexEntry(off OffsetNumber) IndexEntry {
	entryPtr := p.GetEntryPtr(off)
	return *(*IndexEntry)(unsafe.Pointer(&p[entryPtr]))
}

func (p Page) GetDataEntry(off OffsetNumber) DataEntry {
	entryPtr := p.GetEntryPtr(off)
	return *(*DataEntry)(unsafe.Pointer(&p[entryPtr]))
}

// 判断页面的状态

/*
// 判断是否为叶子节点
func (p Page) IsLeaf() bool {
	return p.GetLevel() == 0
}

// 判断是否为最左节点
func (p Page) IsLeftmost() bool {
	return p.GetLeft() == InvalidPageId
}

// 判断是否为最右节点
func (p Page) IsRightmost() bool {
	return p.GetRight() == InvalidPageId
}

// 页面中空闲空间大小
func (p Page) FreeSpaceSize() OffsetNumber {
	upper := OffsetNumber(binary.BigEndian.Uint16(p[upperPos:]))
	lower := OffsetNumber(binary.BigEndian.Uint16(p[lowerPos:]))
	return upper - lower
}

func (p Page) IsExistIndexEntry(pageId PageNumber) bool {
	for off := pageHeaderSize; off < p.GetEndEntryPtrPos(); off += EntryPtrSize {
		entry := p.GetIndexEntry(off)
		if entry.Value() == pageId {
			return true
		}
	}

	return false
}*/

// 将off从页面内的位置转换为数组的形式
func offsetToArray(off OffsetNumber) OffsetNumber {
	return OffsetNumber((off - pageHeaderSize) / EntryPtrSize)
}

// 将off从数组的形式转换为页面内的位置
func arrayToOffset(off OffsetNumber) OffsetNumber {
	return pageHeaderSize + off*EntryPtrSize
}

// io 操作，从文件读取页面
func (p Page) readFile(file *os.File, pageId PageNumber) error {
	if p == nil {
		return errNilPage
	}

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
	if p == nil {
		return errNilPage
	}

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
