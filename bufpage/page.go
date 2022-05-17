package bufpage

import (
	"bytes"
	"errors"
	. "github/suixinpr/ingens/base"
	"github/suixinpr/ingens/storage"
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
)

const (
	// page header size
	pageHeaderSize = OffsetNumber(unsafe.Sizeof(pageHeader{}))

	// PageNumber size
	PageNumberSize = OffsetNumber(unsafe.Sizeof(PageNumber(0)))

	// offset size
	EntryPtrSize = OffsetNumber(unsafe.Sizeof(OffsetNumber(0)))
)

type (
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

	// page header
	pageHeader struct {
		pageId PageNumber

		flag  uint32
		lower OffsetNumber
		upper OffsetNumber
		level uint64

		left  PageNumber
		right PageNumber
	}

	// page
	Page struct {
		ptr unsafe.Pointer
	}
)

// 获取某一层的空页面
func EmptryPage(pageId PageNumber, level uint64) *Page {
	data := make([]byte, PageSize)
	ptr := unsafe.Pointer(&data[0])
	header := (*pageHeader)(ptr)

	header.pageId = pageId
	header.flag = 0
	header.lower = pageHeaderSize
	header.upper = PageDataUpper
	header.level = level

	return &Page{
		ptr: ptr,
	}
}

// 获取页面中的信息

// 获取该页面的id
func (p *Page) GetPageId() PageNumber {
	return (*pageHeader)(p.ptr).pageId
}

// 获取页面中第一个entryPtr的位置
func (p *Page) GetStartEntryPtrPos() OffsetNumber {
	return pageHeaderSize
}

// 获取页面中最后一个entryPtr的位置
func (p *Page) GetEndEntryPtrPos() OffsetNumber {
	return (*pageHeader)(p.ptr).lower
}

func (p *Page) GetEntryPtr(off OffsetNumber) OffsetNumber {
	return *(*OffsetNumber)(unsafe.Pointer(uintptr(p.ptr) + uintptr(off)))
}

func (p *Page) GetHighKey() []byte {
	off := p.GetEndEntryPtrPos() - EntryPtrSize
	entry := p.GetEntry(off)
	return entry.Key()
}

// 根据entryPtr的off获取entry
// 如果是叶子节点则返回DataEntry, 否则返回IndexEntry
func (p *Page) GetEntry(off OffsetNumber) Entry {
	entryPtr := p.GetEntryPtr(off)
	if p.IsLeaf() {
		return &DataEntry{
			ptr: unsafe.Pointer(uintptr(p.ptr) + uintptr(entryPtr)),
		}
	} else {
		return &IndexEntry{
			ptr: unsafe.Pointer(uintptr(p.ptr) + uintptr(entryPtr)),
		}
	}
}

// 获取右兄弟页
func (p *Page) GetRight() PageNumber {
	return (*pageHeader)(p.ptr).right
}

// 获取左兄弟页
func (p *Page) GetLeft() PageNumber {
	return (*pageHeader)(p.ptr).left
}

// 获取左兄弟页
func (p *Page) GetLevel() uint64 {
	return (*pageHeader)(p.ptr).level
}

// 判断页面的状态

// 判断是否为叶子节点
func (p *Page) IsLeaf() bool {
	header := (*pageHeader)(p.ptr)
	return header.level == 0
}

// 判断是否为最左节点
func (p *Page) IsLeftmost() bool {
	return (*pageHeader)(p.ptr).left == InvalidPageId
}

// 判断是否为最右节点
func (p *Page) IsRightmost() bool {
	return (*pageHeader)(p.ptr).right == InvalidPageId
}

// 页面中空闲空间大小
func (p *Page) FreeSpaceSize() OffsetNumber {
	return (*pageHeader)(p.ptr).upper - (*pageHeader)(p.ptr).lower
}

func (p *Page) IsExistIndexEntry(pageId PageNumber) bool {
	header := (*pageHeader)(p.ptr)
	for off := pageHeaderSize; off < header.lower; off += EntryPtrSize {
		entry := p.GetEntry(off).(*IndexEntry)
		if entry.Value() == pageId {
			return true
		}
	}

	return false
}

// 页面的操作

// 将off从页面内的位置转换为数组的形式
func (p *Page) offsetToArray(off OffsetNumber) OffsetNumber {
	return OffsetNumber((off - p.GetStartEntryPtrPos()) / EntryPtrSize)
}

// 将off从数组的形式转换为页面内的位置
func (p *Page) arrayToOffset(off OffsetNumber) OffsetNumber {
	return p.GetStartEntryPtrPos() + off*EntryPtrSize
}

// 页面内二分查找
// 由于页面中key值不允许重复，索引返回的位置指向查找到的key值
// 在最右节点，如果查找的key比页面中所有的Key都大，返回值可能为所有entryptr的右侧
// 第二个返回值为是否找到
func (p *Page) BinarySearch(key []byte) (OffsetNumber, bool) {
	low := p.offsetToArray(p.GetStartEntryPtrPos())
	high := p.offsetToArray(p.GetEndEntryPtrPos())

	// [low, high) binary search
	for low < high {
		mid := (low & high) + (low^high)>>1
		entry := p.GetEntry(p.arrayToOffset(mid))
		result := bytes.Compare(entry.Key(), key)
		if result == 0 {
			return p.arrayToOffset(mid), true
		} else if result < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}

	return p.arrayToOffset(low), false
}

// 插入entry，off为EntryPtr的位置，entry为插入的数据
// 在调用该函数前应该确保off和entry的正确性
func (p *Page) insert(off OffsetNumber, entry Entry) {
	header := (*pageHeader)(p.ptr)
	data := unsafe.Slice((*byte)(p.ptr), PageSize)
	size := entry.Size()

	// 插入entry
	copy(data[header.upper-size:header.upper], entry.Data())
	header.upper -= size

	// 将原有entryPtr向后移动，在off处插入entryPtr
	copy(data[off+EntryPtrSize:header.lower+EntryPtrSize], data[off:header.lower])
	*(*OffsetNumber)(unsafe.Pointer(&data[off])) = header.upper
	header.lower += EntryPtrSize
}

// 插入entry，entry为将要插入的数据
// 页面中不能存在重复的key
func (p *Page) InsertEntry(entry Entry) error {
	if entry == nil {
		return errInvalidEntry
	}

	if entry.Size() > p.FreeSpaceSize() {
		return errLargeEntry
	}

	off, found := p.BinarySearch(entry.Key())
	if found {
		return errRepeatedEntry
	}

	p.insert(off, entry)
	return nil
}

// 重定向索引entry
func (p *Page) RedirectEntry(dst, src PageNumber) error {
	if !p.IsLeaf() {
		return errNotBranch
	}

	if src == dst {
		return errRedirected
	}

	header := (*pageHeader)(p.ptr)
	for off := pageHeaderSize; off <= header.lower; off += EntryPtrSize {
		entry := p.GetEntry(off).(*IndexEntry)
		if entry.Value() == src {
			entry.SetValue(dst)
			return nil
		}
	}

	return errNotFound
}

// 查找拆分位置
func (page *Page) findSplitLoc(insertLoc OffsetNumber, insertSize OffsetNumber) OffsetNumber {
	var leftSize, splicLoc OffsetNumber

	//在左右页面大小相同的情况下，把最后一个entry放在左边
	header := (*pageHeader)(page.ptr)
	splitSize := ((PageDataUpper - header.upper) + (header.lower - pageHeaderSize) + insertSize + 1) / 2

	for off := pageHeaderSize; off <= header.lower; off += EntryPtrSize {
		var size OffsetNumber
		if off < insertLoc {
			/* left of the insertion position */
			size = page.GetEntry(off).Size() + EntryPtrSize
		} else if off > insertLoc {
			/* right of the insertion position */
			size = page.GetEntry(off-EntryPtrSize).Size() + EntryPtrSize
		} else {
			/* the insertion position */
			size = insertSize + EntryPtrSize
		}
		if leftSize+size > splitSize {
			if leftSize+size-splitSize > splitSize-leftSize {
				splicLoc = off
			} else {
				splicLoc = off + EntryPtrSize
			}
			break
		}
		leftSize += size
	}

	return splicLoc
}

// 将page的数据拆分为page和rpage
// 拆分后page为左页面，rpage为右页面
func (page *Page) Split(pageId PageNumber, entry Entry) (*Page, error) {
	// 页面中不能为空
	if page.GetEndEntryPtrPos() == page.GetStartEntryPtrPos() {
		return nil, errEmptyPage
	}

	// 查找插入位置
	insertLoc, found := page.BinarySearch(entry.Key())
	if found {
		return nil, errRepeatedEntry
	}

	header := (*pageHeader)(page.ptr)

	// 初始化化页面，并处理页面头部信息
	lpage := EmptryPage(header.pageId, header.level)
	lheader := (*pageHeader)(lpage.ptr)
	lheader.left = header.left
	lheader.right = pageId

	rpage := EmptryPage(pageId, header.level)
	rheader := (*pageHeader)(rpage.ptr)
	rheader.left = header.pageId
	rheader.right = header.right

	// 查找分裂位置，splitLoc为rpage的第一个entryPtr的位置
	// splitLoc 为插入entry的偏移量
	splitLoc := page.findSplitLoc(insertLoc, entry.Size())

	// 分别处理左右节点数据
	// 循环的为插入entry后的数组
	for off := pageHeaderSize; off <= header.lower; off += EntryPtrSize {
		/* decide which page to put it on */
		if off < insertLoc {
			/* left of the insertion position */
			if off < splitLoc {
				lpage.insert(lheader.lower, page.GetEntry(off))
			} else {
				rpage.insert(rheader.lower, page.GetEntry(off))
			}
		} else if off > insertLoc {
			/* right of the insertion position */
			if off < splitLoc {
				lpage.insert(lheader.lower, page.GetEntry(off-EntryPtrSize))
			} else {
				rpage.insert(rheader.lower, page.GetEntry(off-EntryPtrSize))
			}
		} else {
			/* the insertion position */
			if off < splitLoc {
				lpage.insert(lheader.lower, entry)
			} else {
				rpage.insert(rheader.lower, entry)
			}
		}
	}

	copy(unsafe.Slice((*byte)(page.ptr), PageSize),
		unsafe.Slice((*byte)(lpage.ptr), PageSize))
	return rpage, nil
}

// io 操作
func ReadPage(file *os.File, pageId PageNumber) (*Page, error) {
	offset := int64(pageId) * int64(PageSize)
	buf, err := storage.Read(file, offset, PageSize)
	if err != nil {
		return nil, err
	}
	page := &Page{
		ptr: unsafe.Pointer(&buf[0]),
	}
	return page, nil
}

func WritePage(file *os.File, page *Page) error {
	pageId := page.GetPageId()
	offset := int64(pageId) * int64(PageSize)
	buf := unsafe.Slice((*byte)(page.ptr), PageSize)
	return storage.Write(file, offset, buf)
}
