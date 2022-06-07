package buffer

import (
	"errors"
	"github/suixinpr/ingens/manager/storage"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

var (
	// errBufferCorruption
	errBufferCorruption = errors.New("failed to read this page into cache")
)

type (
	// bufferNumber is set of buf id
	// The index number of the Buffer element, starting from 0
	bufferNumber uint64

	// pageBuffer store bufferElement
	BufferManager struct {
		smgr storage.StorageManager

		bucketNum uint64
		capacity  bufferNumber
		victim    bufferNumber // 受害者
		maxUsage  uint32

		bufferMap  []*bucket // map: mapKey -> bufId
		bufferPool []*Buffer
	}

	// bucket, store actual data
	bucket struct {
		num   uint64
		mu    sync.RWMutex
		items map[string]bufferNumber
	}

	// chunk store page
	Buffer struct {
		key string

		refNum   uint32 // 引用数，赋值操作都在锁住对应的bucket后，原子操作
		usageNum uint32 // usageNum 时钟扫描需要用到的引用数，原子操作

		isDirty bool // 是否为脏页
		isValid bool // 页面是否有效
		isUsed  bool // 该buffer是否被使用过，如果使用过，那么在bufferMap中存在映射

		ioRoutine sync.WaitGroup // 记录io进程
		data      any
	}
)

func NewBufferPool(capacity uint64, bucketNum uint64, bufferSize int, smgr storage.StorageManager) *BufferManager {
	var bmgr = &BufferManager{
		bucketNum: bucketNum,
		capacity:  bufferNumber(capacity),
		victim:    0,
		maxUsage:  5,
	}

	bmgr.bufferMap = make([]*bucket, bucketNum)
	for i := uint64(0); i < bucketNum; i++ {
		bmgr.bufferMap[i] = &bucket{num: i, items: make(map[string]bufferNumber)}
	}

	bmgr.bufferPool = make([]*Buffer, capacity)
	for i := uint64(0); i < capacity; i++ {
		bmgr.bufferPool[i] = &Buffer{isUsed: false}
		bmgr.bufferPool[i].data = smgr.InitData()
	}

	bmgr.smgr = smgr

	return bmgr
}

func (bmgr *BufferManager) getBucket(key string) *bucket {
	h := fnv.New64()
	h.Write([]byte(key))
	return bmgr.bufferMap[h.Sum64()%bmgr.bucketNum]
}

// 获取节点
// pageId 为页面id号
// page 为页面内容
// 如果page == nil，则从file中读取对应的页
func (bmgr *BufferManager) GetBufferData(key string, new bool) (any, error) {
	var newBucket = bmgr.getBucket(key)
	newBucket.mu.RLock()

	// 在缓冲池已经存在对应的Buffer，找到
	if bufId, ok := newBucket.items[key]; ok {
		var buf = bmgr.bufferPool[bufId]

		atomic.AddUint32(&buf.refNum, 1)
		buf.usageNumIncrement(bmgr.maxUsage)
		newBucket.mu.RUnlock()

		// 等待io线程
		buf.ioRoutine.Wait()
		if !buf.isValid {
			atomic.AddUint32(&buf.refNum, ^uint32(0))
			return nil, errBufferCorruption
		}
		return buf.data, nil
	}
	newBucket.mu.RUnlock()

	// 未找到，需要自己获取Buffer
	var bufId bufferNumber
	var buf *Buffer
	var oldBucket *bucket
	for {
		// 获取新的buffer，淘汰算法
		bufId = bmgr.evict()
		buf = bmgr.bufferPool[bufId]
		atomic.AddUint32(&buf.refNum, 1)

		// 找到的是否为空闲buffer
		if !buf.isUsed {
			newBucket.mu.Lock()
			// 是否已经有线程找到buffer了
			if oldBufId, ok := newBucket.items[key]; ok {
				var oldBuf = bmgr.bufferPool[oldBufId]
				atomic.AddUint32(&buf.refNum, ^uint32(0))
				atomic.AddUint32(&oldBuf.refNum, 1)
				oldBuf.usageNumIncrement(bmgr.maxUsage)

				newBucket.mu.Unlock()

				oldBuf.ioRoutine.Wait()
				if !oldBuf.isValid {
					atomic.AddUint32(&oldBuf.refNum, ^uint32(0))
					return nil, errBufferCorruption
				}
				return oldBuf.data, nil
			}
		} else {
			// 写出脏页
			if buf.isDirty {
				buf.isDirty = false
				err := bmgr.smgr.Write(buf.data)
				if err != nil {
					return nil, err
				}
			}

			// 获取旧buffer所在的bucket
			oldBucket = bmgr.getBucket(buf.key)

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
			if oldBufId, ok := newBucket.items[key]; ok {
				var oldBuf = bmgr.bufferPool[oldBufId]
				atomic.AddUint32(&buf.refNum, ^uint32(0))
				atomic.AddUint32(&oldBuf.refNum, 1)
				oldBuf.usageNumIncrement(bmgr.maxUsage)

				oldBucket.mu.Unlock()
				if newBucket.num != oldBucket.num {
					newBucket.mu.Unlock()
				}

				oldBuf.ioRoutine.Wait()
				if !oldBuf.isValid {
					atomic.AddUint32(&oldBuf.refNum, ^uint32(0))
					return nil, errBufferCorruption
				}
				return oldBuf.data, nil
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
		newBucket.items[key] = bufId
		newBucket.mu.Unlock()
	} else {
		// 在bufferMap中删除buffer原有映射，添加新映射
		delete(oldBucket.items, buf.key)
		newBucket.items[key] = bufId

		// 解锁对应的bucket
		oldBucket.mu.Unlock()
		if newBucket.num != oldBucket.num {
			newBucket.mu.Unlock()
		}
	}

	buf.key = key
	buf.isUsed = true

	// 如果不为生成新页面，则IO获取
	if !new {
		err := bmgr.smgr.Read(buf.data)
		if err != nil {
			buf.isValid = false // 获取页面失败
			atomic.AddUint32(&buf.refNum, ^uint32(0))
			return nil, err
		}
	}

	buf.isValid = true
	buf.usageNumIncrement(bmgr.maxUsage)

	return buf.data, nil
}

// 淘汰算法 clock
func (bmgr *BufferManager) evict() bufferNumber {
	for {
		var bufId = bufferNumber(atomic.AddUint64((*uint64)(&bmgr.victim), 1) - 1)
		if bufId >= bmgr.capacity {
			if bufId == bmgr.capacity {
				atomic.AddUint64((*uint64)(&bmgr.victim), ^uint64(bmgr.capacity-1))
			}
			bufId = bufId % bmgr.capacity
		}

		var buf = bmgr.bufferPool[bufId]
		if atomic.LoadUint32(&buf.refNum) == 0 && !buf.usageNumDecrement(bmgr.maxUsage) {
			return bufId
		}
	}
}

func (bmgr *BufferManager) Flush() {
}

// buffer

func (buf *Buffer) usageNumIncrement(maxUsage uint32) {
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

func (buf *Buffer) usageNumDecrement(maxUsage uint32) bool {
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
func (buf *Buffer) Release() {
	atomic.AddUint32(&buf.refNum, ^uint32(0))
}
