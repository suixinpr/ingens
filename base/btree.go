package base

import "unsafe"

type (
	// PageNumber represents the number of a page
	// Page 0 stores meta
	// Index pages should be 1, 2, ... , n
	PageNumber uint64

	// OffsetNumber marks the offset of the data within the page
	// The maximum value is 64KB
	OffsetNumber uint16

	// EntryPosition uniquely identifies the location of an entry
	EntryPosition struct {
		PageId   PageNumber
		EntryPtr OffsetNumber
	}
)

const (
	InvalidPageId PageNumber = 0
)

var (
	// 64KB
	PageSize int = 1 << 16

	// 留出最后校验和的位置
	PageDataUpper OffsetNumber = OffsetNumber(PageSize - int(unsafe.Sizeof(uint64(0))))
)
