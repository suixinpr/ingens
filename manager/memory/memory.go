package memory

import (
	"sync"
)

type MemoryManager struct {
	minSize  uint32
	maxSize  uint32
	base     int
	memChunk []sync.Pool
}

var multiplyDeBruijnBitPosition = [32]int{
	0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
	31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9,
}

func LogBaseTwo(x uint32) int {
	return multiplyDeBruijnBitPosition[x*0x077CB531>>27]
}

func AlignDownPowerOfTwo(x uint32) uint32 {
	x |= x >> 1  //  2
	x |= x >> 2  //  4
	x |= x >> 4  //  8
	x |= x >> 8  // 16
	x |= x >> 16 // 32
	return x - (x >> 1)
}

func AlignUpPowerOfTwo(x uint32) uint32 {
	x -= 1
	x |= x >> 1  //  2
	x |= x >> 2  //  4
	x |= x >> 4  //  8
	x |= x >> 8  // 16
	x |= x >> 16 // 32
	return x + 1
}

// minSize <= maxSize
func NewMemoryManager(minSize, maxSize uint32) *MemoryManager {
	minSize = AlignUpPowerOfTwo(minSize)
	maxSize = AlignDownPowerOfTwo(maxSize)
	minLog := LogBaseTwo(minSize)
	maxLog := LogBaseTwo(maxSize)
	mmgr := &MemoryManager{
		minSize:  minSize,
		maxSize:  maxSize,
		base:     minLog,
		memChunk: make([]sync.Pool, maxLog-minLog+1),
	}
	for i := minLog; i <= maxLog; i++ {
		mmgr.memChunk[i-mmgr.base].New = func(x int) func() any {
			return func() any {
				mem := make([]byte, 1<<x)
				return &mem
			}
		}(i)
	}
	return mmgr
}

// Alloc alloc memory
func (mmgr *MemoryManager) Alloc(size uint32) []byte {
	if size <= mmgr.maxSize {
		if size <= mmgr.minSize {
			mem := mmgr.memChunk[0].Get().(*[]byte)
			return *mem
		} else {
			size = AlignUpPowerOfTwo(size)
			mem := mmgr.memChunk[LogBaseTwo(size)-mmgr.base].Get().(*[]byte)
			return *mem
		}
	}
	return make([]byte, size)
}

// Free free memory
func (mmgr *MemoryManager) Free(mem []byte) {
	if size := uint32(cap(mem)); size <= mmgr.maxSize {
		mmgr.memChunk[LogBaseTwo(size)-mmgr.base].Put(&mem)
	}
}
