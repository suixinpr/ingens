package btnode

import (
	"bytes"
	"encoding/binary"
	"github/suixinpr/ingens/base"
	"os"
)

type Node struct {
	header pageHeader // header is cache
	page   Page
}

func (n *Node) GetPageId() base.PageNumber {
	return n.header.pageId
}

func (n *Node) GetEndOff() base.OffsetNumber {
	return n.header.lower
}

func (n *Node) GetLeft() base.PageNumber {
	return n.header.left
}

func (n *Node) GetRight() base.PageNumber {
	return n.header.right
}

func (n *Node) GetLevel() uint16 {
	return n.header.level
}

// 判断是否为叶子节点
func (n *Node) IsLeaf() bool {
	return n.header.level == 0
}

// 判断是否为最左节点
func (n *Node) IsLeftmost() bool {
	return n.header.left == base.InvalidPageId
}

// 判断是否为最右节点
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

func (n *Node) GetIndexEntry(off base.OffsetNumber) IndexEntry {
	ie := n.page.getIndexEntry(off)
	result := make([]byte, len(ie))
	return result
}

func (n *Node) GetDataEntry(off base.OffsetNumber) DataEntry {
	de := n.page.getDataEntry(off)
	result := make([]byte, len(de))
	return result
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

func (n *Node) Update(off base.OffsetNumber, entry []byte) {

}

func (n *Node) Delete(off base.OffsetNumber) {

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

// // 重定向索引entry
// func (p Page) RedirectEntry(dst, src PageNumber) error {
// 	if !p.IsLeaf() {
// 		return errNotBranch
// 	}

// 	if src == dst {
// 		return errRedirected
// 	}

// 	header := (*pageHeader)(p.ptr)
// 	for off := pageHeaderSize; off <= header.lower; off += EntryPtrSize {
// 		entry := p.GetEntry(off).(*IndexEntry)
// 		if entry.Value() == src {
// 			entry.SetValue(dst)
// 			return nil
// 		}
// 	}

// 	return errNotFound
// }

// // 查找拆分位置
// func (page Page) findSplitLoc(insertLoc OffsetNumber, insertSize OffsetNumber) OffsetNumber {
// 	var leftSize, splicLoc OffsetNumber

// 	//在左右页面大小相同的情况下，把最后一个entry放在左边
// 	header := (*pageHeader)(page.ptr)
// 	splitSize := ((PageDataUpper - header.upper) + (header.lower - pageHeaderSize) + insertSize + 1) / 2

// 	for off := pageHeaderSize; off <= header.lower; off += EntryPtrSize {
// 		var size OffsetNumber
// 		if off < insertLoc {
// 			/* left of the insertion position */
// 			size = page.GetEntry(off).Size() + EntryPtrSize
// 		} else if off > insertLoc {
// 			/* right of the insertion position */
// 			size = page.GetEntry(off-EntryPtrSize).Size() + EntryPtrSize
// 		} else {
// 			/* the insertion position */
// 			size = insertSize + EntryPtrSize
// 		}
// 		if leftSize+size > splitSize {
// 			if leftSize+size-splitSize > splitSize-leftSize {
// 				splicLoc = off
// 			} else {
// 				splicLoc = off + EntryPtrSize
// 			}
// 			break
// 		}
// 		leftSize += size
// 	}

// 	return splicLoc
// }

// // 将page的数据拆分为page和rpage
// // 拆分后page为左页面，rpage为右页面
// func (page Page) Split(pageId PageNumber, entry Entry) (Page, error) {
// 	// 页面中不能为空
// 	if page.GetEndEntryPtrPos() == page.GetStartEntryPtrPos() {
// 		return nil, errEmptyPage
// 	}

// 	// 查找插入位置
// 	insertLoc, found := page.BinarySearch(entry.Key())
// 	if found {
// 		return nil, errRepeatedEntry
// 	}

// 	header := (*pageHeader)(page.ptr)

// 	// 初始化化页面，并处理页面头部信息
// 	lpage := EmptryPage(header.pageId, header.level)
// 	lheader := (*pageHeader)(lpage.ptr)
// 	lheader.left = header.left
// 	lheader.right = pageId

// 	rpage := EmptryPage(pageId, header.level)
// 	rheader := (*pageHeader)(rpage.ptr)
// 	rheader.left = header.pageId
// 	rheader.right = header.right

// 	// 查找分裂位置，splitLoc为rpage的第一个entryPtr的位置
// 	// splitLoc 为插入entry的偏移量
// 	splitLoc := page.findSplitLoc(insertLoc, entry.Size())

// 	// 分别处理左右节点数据
// 	// 循环的为插入entry后的数组
// 	for off := pageHeaderSize; off <= header.lower; off += EntryPtrSize {
// 		/* decide which page to put it on */
// 		if off < insertLoc {
// 			/* left of the insertion position */
// 			if off < splitLoc {
// 				lpage.insert(lheader.lower, page.GetEntry(off))
// 			} else {
// 				rpage.insert(rheader.lower, page.GetEntry(off))
// 			}
// 		} else if off > insertLoc {
// 			/* right of the insertion position */
// 			if off < splitLoc {
// 				lpage.insert(lheader.lower, page.GetEntry(off-EntryPtrSize))
// 			} else {
// 				rpage.insert(rheader.lower, page.GetEntry(off-EntryPtrSize))
// 			}
// 		} else {
// 			/* the insertion position */
// 			if off < splitLoc {
// 				lpage.insert(lheader.lower, entry)
// 			} else {
// 				rpage.insert(rheader.lower, entry)
// 			}
// 		}
// 	}

// 	copy(unsafe.Slice((*byte)(page.ptr), PageSize),
// 		unsafe.Slice((*byte)(lpage.ptr), PageSize))
// 	return rpage, nil
// }

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

func (n *Node) readFile(file *os.File, pageId base.PageNumber) error {
	err := n.page.readFile(file, pageId)
	if err != nil {
		return err
	}

	// temp header
	n.header.pageId = base.PageNumber(binary.BigEndian.Uint64(n.page[pageIdPos:]))
	n.header.lower = base.OffsetNumber(binary.BigEndian.Uint16(n.page[lowerPos:]))
	n.header.upper = base.OffsetNumber(binary.BigEndian.Uint16(n.page[upperPos:]))
	n.header.flag = binary.BigEndian.Uint16(n.page[flagPos:])
	n.header.level = binary.BigEndian.Uint16(n.page[levelPos:])
	n.header.left = base.PageNumber(binary.BigEndian.Uint64(n.page[leftPos:]))
	n.header.right = base.PageNumber(binary.BigEndian.Uint64(n.page[rightPos:]))

	return nil
}

func (n *Node) writeFile(file *os.File) error {
	// temp header
	binary.BigEndian.PutUint64(n.page[pageIdPos:], uint64(n.header.pageId)) // pageId
	binary.BigEndian.PutUint16(n.page[lowerPos:], uint16(n.header.lower))   // lower
	binary.BigEndian.PutUint16(n.page[upperPos:], uint16(n.header.upper))   // upper
	binary.BigEndian.PutUint16(n.page[flagPos:], 0)                         // flag
	binary.BigEndian.PutUint16(n.page[levelPos:], uint16(n.header.level))   // level
	binary.BigEndian.PutUint64(n.page[leftPos:], uint64(n.header.left))     // left
	binary.BigEndian.PutUint64(n.page[rightPos:], uint64(n.header.right))   // right

	return n.page.writeFile(file, n.header.pageId)
}
