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
	memManager := &MemoryManager{
		minSize:  minSize,
		maxSize:  maxSize,
		base:     minLog,
		memChunk: make([]sync.Pool, maxLog-minLog+1),
	}
	for i := minLog; i <= maxLog; i++ {
		memManager.memChunk[i-memManager.base].New = func(x int) func() any {
			return func() any {
				return make([]byte, 1<<x)
			}
		}(i)
	}
	return memManager
}

// Alloc alloc memory
func (memManager *MemoryManager) Alloc(size uint32) []byte {
	if size <= memManager.maxSize {
		if size <= memManager.minSize {
			return memManager.memChunk[0].Get().([]byte)
		} else {
			size = AlignUpPowerOfTwo(size)
			return memManager.memChunk[LogBaseTwo(size)-memManager.base].Get().([]byte)
		}
	}
	return make([]byte, size)
}

// Free free memory
func (memManager *MemoryManager) Free(mem []byte) {
	if size := uint32(cap(mem)); size <= memManager.maxSize {
		memManager.memChunk[LogBaseTwo(size)-memManager.base].Put(mem)
	}
}
