package bufpage

import (
	"errors"
	. "github/suixinpr/ingens/base"
	"os"
	"sync"
	"sync/atomic"
)

var (
	// errZeroBufferCapacity
	errZeroBufferCapacity = errors.New("The capacity of the buffer cannot be 0")

	// errZeroBucketNum
	errZeroBucketNum = errors.New("Bucket Num cannot be 0")

	// errSmallBufferCapacity
	errSmallBufferCapacity = errors.New("The capacity of the buffer cannot be less than the number of buckets")

	// errBufferCorruption
	errBufferCorruption = errors.New("Failed to read this page into cache")
)

type (
	// bufferNumber is set of buf id
	// The index number of the buffer element, starting from 0
	bufferNumber uint64

	// pageBuffer store bufferElement
	BufferPool struct {
		bucketNum uint64
		capacity  bufferNumber
		victim    bufferNumber // 受害者
		maxUsage  uint32

		bufferMap []*bucket // map: pageId -> bufId
		buffers   []*buffer
	}

	// bucket, store actual data
	bucket struct {
		num   uint64
		mu    sync.RWMutex
		items map[PageNumber]bufferNumber
	}

	// chunk store node
	buffer struct {
		pageId PageNumber

		refNum   uint32 // 引用数，赋值操作都在锁住对应的bucket后，原子操作
		usageNum uint32 // usageNum 时钟扫描需要用到的引用数，原子操作

		isDirty bool // 是否为脏页
		isValid bool // 页面是否有效
		isUsed  bool // 该buffer是否被使用过，如果使用过，那么在bufferMap中存在映射

		ioRoutine sync.WaitGroup // 记录io进程
		node      Node
	}
)

func NewBufferPool(capacity uint64, bucketNum uint64) (*BufferPool, error) {
	if capacity == 0 {
		return nil, errZeroBufferCapacity
	}

	if bucketNum == 0 {
		return nil, errZeroBucketNum
	}

	if capacity < bucketNum {
		return nil, errSmallBufferCapacity
	}

	var bufPool = &BufferPool{
		bucketNum: bucketNum,
		capacity:  bufferNumber(capacity),
		victim:    0,
		maxUsage:  5,
	}

	bufPool.bufferMap = make([]*bucket, bucketNum)
	for i := uint64(0); i < bucketNum; i++ {
		bufPool.bufferMap[i] = &bucket{num: i, items: make(map[PageNumber]bufferNumber)}
	}

	bufPool.buffers = make([]*buffer, capacity)
	for i := uint64(0); i < capacity; i++ {
		bufPool.buffers[i] = &buffer{isUsed: false}
		bufPool.buffers[i].node.page = make(Page, PageSize)
	}

	return bufPool, nil
}

func (bufPool *BufferPool) getBucket(key PageNumber) *bucket {
	return bufPool.bufferMap[uint64(key)%bufPool.bucketNum]
}

// 获取节点
// pageId 为页面id号
// page 为页面内容
// 如果page == nil，则从file中读取对应的页
func (bufPool *BufferPool) GetNode(pageId PageNumber, new bool, file *os.File) (*Node, error) {
	var newBucket = bufPool.getBucket(pageId)
	newBucket.mu.RLock()

	// 在缓冲池已经存在对应的Buffer，找到
	if bufId, ok := newBucket.items[pageId]; ok {
		var buf = bufPool.buffers[bufId]

		atomic.AddUint32(&buf.refNum, 1)
		buf.usageNumIncrement(bufPool.maxUsage)
		newBucket.mu.RUnlock()

		// 等待io线程
		buf.ioRoutine.Wait()
		if !buf.isValid {
			atomic.AddUint32(&buf.refNum, ^uint32(0))
			return nil, errBufferCorruption
		}
		return &buf.node, nil
	}
	newBucket.mu.RUnlock()

	// 未找到，需要自己获取Buffer
	var bufId bufferNumber
	var buf *buffer
	var oldBucket *bucket
	for {
		// 获取新的buffer，淘汰算法
		bufId = bufPool.evict()
		buf = bufPool.buffers[bufId]
		atomic.AddUint32(&buf.refNum, 1)

		// 找到的是否为空闲buffer
		if !buf.isUsed {
			newBucket.mu.Lock()
			// 是否已经有线程找到buffer了
			if oldBufId, ok := newBucket.items[pageId]; ok {
				var oldBuf = bufPool.buffers[oldBufId]
				atomic.AddUint32(&buf.refNum, ^uint32(0))
				atomic.AddUint32(&oldBuf.refNum, 1)
				oldBuf.usageNumIncrement(bufPool.maxUsage)

				newBucket.mu.Unlock()

				oldBuf.ioRoutine.Wait()
				if !oldBuf.isValid {
					atomic.AddUint32(&oldBuf.refNum, ^uint32(0))
					return nil, errBufferCorruption
				}
				return &oldBuf.node, nil
			}
		} else {
			// 写出脏页
			if buf.isDirty {
				buf.isDirty = false
				err := buf.node.writeFile(file)
				if err != nil {
					// log
				}
			}

			// 获取旧buffer所在的bucket
			oldBucket = bufPool.getBucket(buf.pageId)

			// 从左往右锁住bucekt，避免死锁
			if oldBucket.num < newBucket.num {
				oldBucket.mu.Lock()
				newBucket.mu.Lock()
			} else if oldBucket.num > newBucket.num {
				newBucket.mu.Lock()
				oldBucket.mu.Lock()
			} else {
				newBucket.mu.Lock()
			}

			// 如果已经有线程找到buffer了，那么返回它并撤销我们之前做的操作
			if oldBufId, ok := newBucket.items[pageId]; ok {
				var oldBuf = bufPool.buffers[oldBufId]
				atomic.AddUint32(&buf.refNum, ^uint32(0))
				atomic.AddUint32(&oldBuf.refNum, 1)
				oldBuf.usageNumIncrement(bufPool.maxUsage)

				oldBucket.mu.Unlock()
				if newBucket.num != oldBucket.num {
					newBucket.mu.Unlock()
				}

				oldBuf.ioRoutine.Wait()
				if !oldBuf.isValid {
					atomic.AddUint32(&oldBuf.refNum, ^uint32(0))
					return nil, errBufferCorruption
				}
				return &oldBuf.node, nil
			}
		}

		// 是否有其他线程引用该缓存区
		if atomic.LoadUint32(&buf.refNum) == 1 {
			break
		}

		// 如果线程进行到这里，那么只能重新获取
		if !buf.isUsed {
			newBucket.mu.Unlock()
		} else {
			oldBucket.mu.Unlock()
			if newBucket.num != oldBucket.num {
				newBucket.mu.Unlock()
			}
		}

		atomic.AddUint32(&buf.refNum, ^uint32(0))
	}

	// Okay, it's finally safe to rename the buffer.

	// 添加io写入任务
	buf.ioRoutine.Add(1)
	defer buf.ioRoutine.Done()

	// 修改bufferMap
	if !buf.isUsed {
		newBucket.items[pageId] = bufId
		newBucket.mu.Unlock()
	} else {
		// 在bufferMap中删除buffer原有映射，添加新映射
		delete(oldBucket.items, buf.pageId)
		newBucket.items[pageId] = bufId

		// 解锁对应的bucket
		oldBucket.mu.Unlock()
		if newBucket.num != oldBucket.num {
			newBucket.mu.Unlock()
		}
	}

	buf.pageId = pageId
	buf.isUsed = true

	// 如果不为生成新页面，则IO获取
	if !new {
		err := buf.node.readFile(file, pageId)
		if err != nil {
			buf.node.page = nil
			buf.isValid = false // 获取页面失败
			atomic.AddUint32(&buf.refNum, ^uint32(0))
			return nil, err
		}
	}

	buf.node.bufid = bufId
	buf.isValid = true
	buf.usageNumIncrement(bufPool.maxUsage)

	return &buf.node, nil
}

// 淘汰算法 clock
func (bufPool *BufferPool) evict() bufferNumber {
	for {
		var bufId = bufferNumber(atomic.AddUint64((*uint64)(&bufPool.victim), 1) - 1)
		if bufId >= bufPool.capacity {
			if bufId == bufPool.capacity {
				atomic.AddUint64((*uint64)(&bufPool.victim), ^uint64(bufPool.capacity-1))
			}
			bufId = bufId % bufPool.capacity
		}

		var buf = bufPool.buffers[bufId]
		if atomic.LoadUint32(&buf.refNum) == 0 && !buf.usageNumDecrement(bufPool.maxUsage) {
			return bufId
		}
	}
}

func (bufPool *BufferPool) Flush() {
}

// buffer

func (buf *buffer) usageNumIncrement(maxUsage uint32) {
	for {
		if atomic.LoadUint32(&buf.usageNum) == maxUsage {
			return
		}
		for i := uint32(0); i < maxUsage; i++ {
			if atomic.CompareAndSwapUint32(&buf.usageNum, i, i+1) {
				return
			}
		}
	}
}

func (buf *buffer) usageNumDecrement(maxUsage uint32) bool {
	for {
		if atomic.LoadUint32(&buf.usageNum) == 0 {
			return false
		}
		for i := uint32(0); i < maxUsage; i++ {
			if atomic.CompareAndSwapUint32(&buf.usageNum, i+1, i) {
				return true
			}
		}
	}
}

// 释放对该节点的引用
func (buf *buffer) Release() {
	atomic.AddUint32(&buf.refNum, ^uint32(0))
}
