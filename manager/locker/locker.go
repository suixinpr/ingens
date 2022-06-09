package locker

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// schedule
	LockerManager struct {
		bucketNum uint64
		timeout   time.Duration

		resourceMap  []*bucket
		resourcePool sync.Pool
	}

	// bucket
	bucket struct {
		mu    sync.RWMutex
		items map[string]*lock
	}

	// lock
	lock struct {
		acquireNum uint32
		locked     chan struct{}
	}
)

func NewLockerManager(bucketNum uint64, timeout time.Duration) *LockerManager {
	lm := &LockerManager{
		bucketNum: bucketNum,
		timeout:   timeout,
	}
	lm.resourceMap = make([]*bucket, bucketNum)
	for i := uint64(0); i < bucketNum; i++ {
		lm.resourceMap[i] = &bucket{items: make(map[string]*lock)}
	}
	lm.resourcePool = sync.Pool{
		New: func() any {
			return new(lock)
		},
	}
	return lm
}

func (s *LockerManager) getBucket(key []byte) *bucket {
	h := fnv.New64()
	h.Write(key)
	return s.resourceMap[h.Sum64()%s.bucketNum]
}

func (s *LockerManager) Lock(key []byte) bool {
	b := s.getBucket(key)
	k := string(key)

	// 获取资源
	b.mu.RLock()
	res, ok := b.items[k]
	if ok {
		atomic.AddUint32(&res.acquireNum, 1)
	}
	b.mu.RUnlock()

	// 如果没找到，添加资源
	if !ok {
		b.mu.Lock()
		if res, ok = b.items[k]; ok {
			atomic.AddUint32(&res.acquireNum, 1)
			b.mu.Unlock()
		} else {
			res = s.resourcePool.Get().(*lock)
			atomic.StoreUint32(&res.acquireNum, 1)
			res.locked = make(chan struct{}, 1)
			b.items[k] = res
			b.mu.Unlock()
		}
	}

	select {
	case res.locked <- struct{}{}:
		return true
	case <-time.After(s.timeout):
		return false
	}
}

func (s *LockerManager) Unlock(key []byte) {
	b := s.getBucket(key)
	b.mu.RLock()
	res := b.items[string(key)]
	b.mu.RUnlock()
	<-res.locked
	atomic.AddUint32(&res.acquireNum, ^uint32(0))
}

func (s *LockerManager) Clean() int {
	var num int
	for i := uint64(0); i < s.bucketNum; i++ {
		b := s.resourceMap[i]
		b.mu.Lock()
		for k, v := range b.items {
			if atomic.LoadUint32(&v.acquireNum) == 0 {
				delete(b.items, k)
				close(v.locked)
				s.resourcePool.Put(v)
				num += 1
			}
		}
		b.mu.Unlock()
	}
	return num
}
