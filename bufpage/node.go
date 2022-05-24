package bufpage

import (
	"encoding/binary"
	. "github/suixinpr/ingens/base"
	"os"
	"sync"
)

type Node struct {
	bufid bufferNumber

	mu     sync.RWMutex // 该页面是否正在被读取或写入
	header pageHeader   // header is cache
	page   Page
}

// // 页面内二分查找
// // 由于页面中key值不允许重复，索引返回的位置指向查找到的key值
// // 在最右节点，如果查找的key比页面中所有的Key都大，返回值可能为所有entryptr的右侧
// // 第二个返回值为是否找到
// func (p Page) BinarySearch(key []byte) (OffsetNumber, bool) {
// 	low := offsetToArray(p.GetStartEntryPtrPos())
// 	high := offsetToArray(p.GetEndEntryPtrPos())

// 	// [low, high) binary search
// 	for low < high {
// 		mid := (low & high) + (low^high)>>1
// 		result := bytes.Compare(p.GetKey(mid), key)
// 		if result == 0 {
// 			return arrayToOffset(mid), true
// 		} else if result < 0 {
// 			low = mid + 1
// 		} else {
// 			high = mid
// 		}
// 	}

// 	return arrayToOffset(low), false
// }

// // 插入entry，off为EntryPtr的位置，entry为插入的数据
// // 在调用该函数前应该确保off和entry的正确性
// func (p Page) insert(off OffsetNumber, entry Entry) {
// 	header := (*pageHeader)(p.ptr)
// 	data := unsafe.Slice((*byte)(p.ptr), PageSize)
// 	size := entry.Size()

// 	// 插入entry
// 	copy(data[header.upper-size:header.upper], entry.Data())
// 	header.upper -= size

// 	// 将原有entryPtr向后移动，在off处插入entryPtr
// 	copy(data[off+EntryPtrSize:header.lower+EntryPtrSize], data[off:header.lower])
// 	*(*OffsetNumber)(unsafe.Pointer(&data[off])) = header.upper
// 	header.lower += EntryPtrSize
// }

// // 插入entry，entry为将要插入的数据
// // 页面中不能存在重复的key
// func (p Page) InsertEntry(entry Entry) error {
// 	if entry == nil {
// 		return errInvalidEntry
// 	}

// 	if entry.Size() > p.FreeSpaceSize() {
// 		return errLargeEntry
// 	}

// 	off, found := p.BinarySearch(entry.Key())
// 	if found {
// 		return errRepeatedEntry
// 	}

// 	p.insert(off, entry)
// 	return nil
// }

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

func (n *Node) readFile(file *os.File, pageId PageNumber) error {
	err := n.page.readFile(file, pageId)
	if err != nil {
		return err
	}

	// temp header
	n.header.pageId = PageNumber(binary.BigEndian.Uint64(n.page[pageIdPos:]))
	n.header.lower = OffsetNumber(binary.BigEndian.Uint16(n.page[lowerPos:]))
	n.header.upper = OffsetNumber(binary.BigEndian.Uint16(n.page[upperPos:]))
	n.header.flag = binary.BigEndian.Uint16(n.page[flagPos:])
	n.header.level = binary.BigEndian.Uint16(n.page[levelPos:])
	n.header.left = PageNumber(binary.BigEndian.Uint64(n.page[leftPos:]))
	n.header.right = PageNumber(binary.BigEndian.Uint64(n.page[rightPos:]))

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
