package buffer

import (
	. "github/suixinpr/ingens/base"
	"strconv"
	"testing"
)

func TestNewBufferPool(t *testing.T) {
	test := []struct {
		name string

		capacity  uint64
		bucketNum uint64
	}{
		{"DefaultBufferPool", 2048, 256},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			bufPool, err := NewBufferPool(tt.capacity, tt.bucketNum)
			if err != nil {
				t.Errorf("NewBufferPool() err: %v", err)
			}
			if len(bufPool.buffers) != int(tt.capacity) {
				t.Errorf("NewBufferPool() capacity: got = %v, want = %v", len(bufPool.buffers), int(tt.capacity))
			}
			if len(bufPool.bufferMap) != int(tt.bucketNum) {
				t.Errorf("NewBufferPool() bucketNum: got = %v, want = %v", len(bufPool.bufferMap), int(tt.bucketNum))
			}
		})
	}
}

func TestGetNode(t *testing.T) {
	test := []struct {
		name string

		pageId PageNumber
	}{
		{"1", 1},
		{"2", 2},
		{"3", 3},
		{"4", 4},
		{"5", 5},
		{"6", 6},
		{"7", 7},
		{"8", 8},
	}

	bufPool, err := NewBufferPool(4, 2)
	if err != nil {
		t.Errorf("TestGetNode() NewBufferPool err: %v", err)
		return
	}
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := bufPool.GetNode(tt.pageId, true, nil)
			if err != nil {
				t.Errorf("TestGetNode() err: %v", err)
			}
			if buf.pageId != tt.pageId {
				t.Errorf("TestGetNode() pageId: got = %v, want = %v", buf.pageId, tt.pageId)
			}
			buf.Release()
		})
	}
}

func TestParallelGetNode(t *testing.T) {
	processNum := 100
	test := []struct {
		name string

		pageId PageNumber
	}{
		{"1", 1},
		{"2", 2},
		{"3", 3},
		{"4", 4},
		{"5", 5},
		{"6", 6},
		{"7", 7},
		{"8", 8},
		{"1", 1},
		{"2", 2},
		{"3", 3},
		{"4", 4},
		{"5", 5},
		{"6", 6},
		{"7", 7},
		{"8", 8},
		{"1", 1},
		{"2", 2},
		{"3", 3},
		{"4", 4},
		{"5", 5},
		{"6", 6},
		{"7", 7},
		{"8", 8},
	}

	bufPool, err := NewBufferPool(4, 4)
	if err != nil {
		t.Errorf("TestGetNode() NewBufferPool err: %v", err)
		return
	}

	for i := 0; i < processNum; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			for _, tt := range test {
				ep := EmptryPage(tt.pageId, 0)
				n, err := bufPool.GetNode(tt.pageId, true, nil)
				if err != nil {
					t.Errorf("TestGetNode() err: %v", err)
				}
				if buf.pageId != tt.pageId {
					t.Errorf("TestGetNode() pageId: got = %v, want = %v", buf.pageId, tt.pageId)
				}
				buf.Release()
			}
		})
	}
}
