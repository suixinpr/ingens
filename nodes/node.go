package nodes

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/manager/buffer"
	"sync"
)

var (
	errNotFound  = errors.New("")
	errSplitNode = errors.New("")
)

const (
	SPLIT_INSERT uint8 = iota
	SPLIT_UPDATE
)

type Node struct {
	mu      sync.RWMutex
	buf     *buffer.Buffer
	isDirty bool

	header pageHeader // header is cache
	page   Page
}

func NewNode() *Node {
	return &Node{page: make(Page, base.PageSize)}
}

func (n *Node) Init(pageId base.PageNumber, level uint16) {
	n.header.pageId = pageId
	n.header.lower = pageHeaderSize
	n.header.upper = base.PageDataUpper
	n.header.level = level
}

// get

func (n *Node) GetPageId() base.PageNumber {
	return n.header.pageId
}

func (n *Node) GetEndOff() base.OffsetNumber {
	return n.header.lower
}

func (n *Node) GetLevel() uint16 {
	return n.header.level
}

func (n *Node) GetLeft() base.PageNumber {
	return n.header.left
}

func (n *Node) GetRight() base.PageNumber {
	return n.header.right
}

// is
func (n *Node) IsLeaf() bool {
	return n.header.level == 0
}

func (n *Node) IsLeftmost() bool {
	return n.header.left == base.InvalidPageId
}

func (n *Node) IsRightmost() bool {
	return n.header.right == base.InvalidPageId
}

// 判断是否存在对应index entry
func (n *Node) IsExistIndexEntry(pageId base.PageNumber) bool {
	if n.IsLeaf() {
		return false
	}
	for off := pageHeaderSize; off < n.header.lower; off += EntryPtrSize {
		entry := n.page.getIndexEntry(off)
		if entry.Value() == pageId {
			return true
		}
	}
	return false
}

// 页面中空闲空间大小
func (n *Node) FreeSpaceSize() base.OffsetNumber {
	return n.header.upper - n.header.lower
}

func (n *Node) GetEntrySize(off base.OffsetNumber) base.OffsetNumber {
	if n.IsLeaf() {
		e := n.page.getDataEntry(off)
		return e.Size()
	} else {
		e := n.page.getIndexEntry(off)
		return e.Size()
	}
}

func (n *Node) GetKey(off base.OffsetNumber) []byte {
	if n.IsLeaf() {
		e := n.page.getDataEntry(off)
		return e.Key()
	} else {
		e := n.page.getIndexEntry(off)
		return e.Key()
	}
}

func (n *Node) GetHighKey() []byte {
	off := n.header.lower - EntryPtrSize
	return n.GetKey(off)
}

func (n *Node) GetEntry(off base.OffsetNumber) []byte {
	if n.IsLeaf() {
		return n.page.getDataEntry(off)
	} else {
		return n.page.getIndexEntry(off)
	}
}

func (n *Node) GetIndexEntry(off base.OffsetNumber) IndexEntry {
	return n.page.getIndexEntry(off)
}

func (n *Node) GetDataEntry(off base.OffsetNumber) DataEntry {
	return n.page.getDataEntry(off)
}

// 插入entry，off为EntryPtr的位置，entry为插入的数据
// 在调用该函数前应该确保off和entry的正确性
func (n *Node) Insert(off base.OffsetNumber, entry []byte) {
	size := base.OffsetNumber(len(entry))

	// 插入entry
	copy(n.page[n.header.upper-size:n.header.upper], entry)
	n.header.upper -= size

	// 将原有entryPtr向后移动，在off处插入entryPtr
	copy(n.page[off+EntryPtrSize:n.header.lower+EntryPtrSize], n.page[off:n.header.lower])
	binary.BigEndian.PutUint16(n.page[off:], uint16(n.header.upper))
	n.header.lower += EntryPtrSize
}

// Entry
func (n *Node) InsertDataEntry(off base.OffsetNumber, entry DataEntry) {

}

// Entry
func (n *Node) InsertIndexEntry(off base.OffsetNumber, entry IndexEntry) {

}

// 页面内二分查找
// 由于页面中key值不允许重复，索引返回的位置指向查找到的key值
// 在最右节点，如果查找的key比页面中所有的Key都大，返回值可能为所有entryptr的右侧
// 第二个返回值为是否找到
func (n *Node) BinarySearch(key []byte) (base.OffsetNumber, bool) {
	low := offsetToArray(pageHeaderSize)
	high := offsetToArray(n.header.lower)

	// [low, high) binary search
	for low < high {
		mid := (low & high) + (low^high)>>1
		result := bytes.Compare(n.GetKey(mid), key)
		if result == 0 {
			return arrayToOffset(mid), true
		} else if result < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}

	return arrayToOffset(low), false
}

// 重定向索引entry
func (n *Node) RedirectEntry(dst, src base.PageNumber) error {
	for off := pageHeaderSize; off < n.header.lower; off += EntryPtrSize {
		entry := n.page.getIndexEntry(off)
		if entry.Value() == src {
			entry.UpdateValue(dst)
			return nil
		}
	}
	return errNotFound
}

// 将page的数据拆分为page和rpage
// 拆分后page为左页面，rpage为右页面
func (n *Node) Split(rn *Node, insertLoc base.OffsetNumber, insertSize base.OffsetNumber, entry []byte, opr uint8) error {
	// 页面中至少需要两个entry
	if offsetToArray(n.header.upper) < 2 {
		return errSplitNode
	}

	ln := NewNode()
	ln.Init(n.header.pageId, n.header.level)

	ln.header.left = n.header.left
	ln.header.right = rn.header.pageId

	rn.header.left = n.header.pageId
	rn.header.right = n.header.right

	switch opr {
	case SPLIT_INSERT:
		splitLoc := n.findSplitLocForInsert(insertLoc, insertSize)
		n.splitForInsert(ln, rn, insertLoc, splitLoc, entry)
	case SPLIT_UPDATE:
		splitLoc := n.findSplitLocForUpdate(insertLoc, insertSize)
		n.splitForUpdate(ln, rn, insertLoc, splitLoc, entry)
	default:
		return errSplitNode
	}

	// finish
	ln.WriteHeaderToPage()
	copy(n.page, ln.page)
	n.WritePageToHeader()
	return nil
}

// 查找拆分位置
func (n *Node) findSplitLocForInsert(insertLoc base.OffsetNumber, insertSize base.OffsetNumber) base.OffsetNumber {
	var leftSize, splicLoc base.OffsetNumber

	//在左右页面大小相同的情况下，把最后一个entry放在左边
	splitSize := ((base.PageDataUpper - n.header.upper) + (n.header.lower - pageHeaderSize) + insertSize + 1) / 2

	for off := pageHeaderSize; off <= n.header.lower; off += EntryPtrSize {
		var size base.OffsetNumber
		if off < insertLoc {
			/* left of the insertion position */
			size = n.GetEntrySize(off) + EntryPtrSize
		} else if off > insertLoc {
			/* right of the insertion position */
			size = n.GetEntrySize(off-EntryPtrSize) + EntryPtrSize
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

// 查找拆分位置
func (n *Node) findSplitLocForUpdate(insertLoc base.OffsetNumber, insertSize base.OffsetNumber) base.OffsetNumber {
	var leftSize, splicLoc base.OffsetNumber

	//在左右页面大小相同的情况下，把最后一个entry放在左边
	splitSize := ((base.PageDataUpper - n.header.upper) + (n.header.lower - pageHeaderSize) + insertSize + 1) / 2

	for off := pageHeaderSize; off <= n.header.lower; off += EntryPtrSize {
		var size base.OffsetNumber
		if off < insertLoc {
			/* left of the insertion position */
			size = n.GetEntrySize(off) + EntryPtrSize
		} else if off > insertLoc {
			/* right of the insertion position */
			size = n.GetEntrySize(off-EntryPtrSize) + EntryPtrSize
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

func (n *Node) splitForInsert(ln, rn *Node, insertLoc, splitLoc base.OffsetNumber, entry []byte) {
	// 分别处理左右节点数据
	// 循环的为插入entry后的数组
	for off := pageHeaderSize; off <= n.header.lower; off += EntryPtrSize {
		/* decide which page to put it on */
		if off < insertLoc {
			/* left of the insertion position */
			if off < splitLoc {
				ln.Insert(ln.header.lower, n.GetEntry(off))
			} else {
				rn.Insert(rn.header.lower, n.GetEntry(off))
			}
		} else if off > insertLoc {
			/* right of the insertion position */
			if off < splitLoc {
				ln.Insert(ln.header.lower, n.GetEntry(off-EntryPtrSize))
			} else {
				rn.Insert(rn.header.lower, n.GetEntry(off-EntryPtrSize))
			}
		} else {
			/* the insertion position */
			if off < splitLoc {
				ln.Insert(ln.header.lower, entry)
			} else {
				rn.Insert(rn.header.lower, entry)
			}
		}
	}
}

func (n *Node) splitForUpdate(ln, rn *Node, insertLoc, splitLoc base.OffsetNumber, entry []byte) {
	// 分别处理左右节点数据
	// 循环的为插入entry后的数组
	for off := pageHeaderSize; off <= n.header.lower; off += EntryPtrSize {
		/* decide which page to put it on */
		if off < insertLoc {
			/* left of the insertion position */
			if off < splitLoc {
				ln.Insert(ln.header.lower, n.GetEntry(off))
			} else {
				rn.Insert(rn.header.lower, n.GetEntry(off))
			}
		} else if off > insertLoc {
			/* right of the insertion position */
			if off < splitLoc {
				ln.Insert(ln.header.lower, n.GetEntry(off-EntryPtrSize))
			} else {
				rn.Insert(rn.header.lower, n.GetEntry(off-EntryPtrSize))
			}
		} else {
			/* the insertion position */
			if off < splitLoc {
				ln.Insert(ln.header.lower, entry)
			} else {
				rn.Insert(rn.header.lower, entry)
			}
		}
	}
}

// lock

func (n *Node) RLock() {
	n.mu.RLock()
}

func (n *Node) RUnlock() {
	n.mu.RUnlock()
}

func (n *Node) Lock() {
	n.mu.Lock()
}

func (n *Node) Unlock() {
	n.mu.Unlock()
}

func (n *Node) Release() {
	n.buf.Release()
}

// IO

func (n *Node) WritePageToHeader() {
	n.header.pageId = base.PageNumber(binary.BigEndian.Uint64(n.page[pageIdPos:])) // pageId
	n.header.lower = base.OffsetNumber(binary.BigEndian.Uint16(n.page[lowerPos:])) // lower
	n.header.upper = base.OffsetNumber(binary.BigEndian.Uint16(n.page[upperPos:])) // upper
	n.header.level = binary.BigEndian.Uint16(n.page[levelPos:])                    // level
	n.header.left = base.PageNumber(binary.BigEndian.Uint64(n.page[leftPos:]))     // left
	n.header.right = base.PageNumber(binary.BigEndian.Uint64(n.page[rightPos:]))   // right
}

func (n *Node) WriteHeaderToPage() {
	binary.BigEndian.PutUint64(n.page[pageIdPos:], uint64(n.header.pageId)) // pageId
	binary.BigEndian.PutUint16(n.page[lowerPos:], uint16(n.header.lower))   // lower
	binary.BigEndian.PutUint16(n.page[upperPos:], uint16(n.header.upper))   // upper
	binary.BigEndian.PutUint16(n.page[levelPos:], uint16(n.header.level))   // level
	binary.BigEndian.PutUint64(n.page[leftPos:], uint64(n.header.left))     // left
	binary.BigEndian.PutUint64(n.page[rightPos:], uint64(n.header.right))   // right
}
