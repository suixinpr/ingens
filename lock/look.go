package lock

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrTimeout = errors.New("Timeout for acquiring entry lock")

	ErrReleased = errors.New("The lock has been released")
)

type (
	LockerManager struct {
		bucketNum uint64
		timeout   time.Duration

		resourceMap  []*bucket
		resourcePool sync.Pool
	}

	// bucket
	bucket struct {
		mu    sync.RWMutex
		items map[string]*resource
	}

	resource struct {
		acquireNum uint32
		locked     chan struct{}
	}

	Locker struct {
		res      *resource // 锁住的资源
		released uint32
	}
)

func NewLockerManager(bucketNum uint64, timeout time.Duration) *LockerManager {
	lm := &LockerManager{
		bucketNum: bucketNum,
		timeout:   timeout,
	}
	lm.resourceMap = make([]*bucket, bucketNum)
	for i := uint64(0); i < bucketNum; i++ {
		lm.resourceMap[i] = &bucket{items: make(map[string]*resource)}
	}
	lm.resourcePool = sync.Pool{
		New: func() any {
			return new(resource)
		},
	}
	return lm
}

func (lm *LockerManager) getBucket(key []byte) *bucket {
	h := fnv.New64()
	h.Write(key)
	return lm.resourceMap[h.Sum64()%lm.bucketNum]
}

func (lm *LockerManager) Lock(key []byte) (*Locker, error) {
	b := lm.getBucket(key)
	s := string(key)

	// 获取资源
	b.mu.RLock()
	res, ok := b.items[s]
	if ok {
		atomic.AddUint32(&res.acquireNum, 1)
	}
	b.mu.RUnlock()

	// 如果没找到，添加资源
	if !ok {
		b.mu.Lock()
		if res, ok = b.items[s]; ok {
			atomic.AddUint32(&res.acquireNum, 1)
			b.mu.Unlock()
		} else {
			res = lm.resourcePool.Get().(*resource)
			res.locked = make(chan struct{}, 1)
			atomic.StoreUint32(&res.acquireNum, 1)
			b.items[s] = res
			fmt.Println(s, res)
			b.mu.Unlock()
		}
	}

	// 获取资源锁
	return res.acquireLock(lm.timeout)
}

func (res *resource) acquireLock(timeout time.Duration) (*Locker, error) {
	select {
	case res.locked <- struct{}{}:
		l := &Locker{res: res}
		return l, nil
	case <-time.After(timeout):
		atomic.AddUint32(&res.acquireNum, ^uint32(0))
		return nil, ErrTimeout
	}
}

func (l *Locker) Unlock() error {
	if !atomic.CompareAndSwapUint32(&l.released, 0, 1) {
		return ErrReleased
	}
	<-l.res.locked
	atomic.AddUint32(&l.res.acquireNum, ^uint32(0))
	return nil
}

func (lm *LockerManager) Clean() int {
	var num int
	for i := uint64(0); i < lm.bucketNum; i++ {
		b := lm.resourceMap[i]
		b.mu.Lock()
		for k, v := range b.items {
			if atomic.LoadUint32(&v.acquireNum) == 0 {
				delete(b.items, k)
				close(v.locked)
				lm.resourcePool.Put(v)
				num += 1
			}
		}
		b.mu.Unlock()
	}
	return num
}
