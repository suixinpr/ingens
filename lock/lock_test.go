package lock

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestLockAndUnlock(t *testing.T) {
	test := []struct {
		name string

		key []byte
	}{
		{"1", []byte("1")},
		{"2", []byte("2")},
		{"3", []byte("3")},
		{"4", []byte("4")},
		{"5", []byte("5")},
		{"6", []byte("6")},
		{"7", []byte("7")},
		{"8", []byte("8")},
	}

	lm := NewLockerManager(4, 1*time.Second)
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			l, err := lm.Lock(tt.key)
			if err != nil {
				t.Errorf("Lock() err: %v", err)
			}
			err = l.Unlock()
			if err != nil {
				t.Errorf("Lock() err: %v", err)
			}
		})
	}
	n := lm.Clean()
	if n != len(test) {
		t.Errorf("TestGetNode() pageId: got = %v, want = %v", n, len(test))
	}
}

func TestParallelLockAndUnlock(t *testing.T) {
	processNum := 100
	test := []struct {
		name string

		key []byte
	}{
		{"1", []byte("1")},
		{"2", []byte("2")},
		{"3", []byte("3")},
		{"4", []byte("4")},
		{"5", []byte("5")},
		{"6", []byte("6")},
		{"7", []byte("7")},
		{"8", []byte("8")},
	}

	lm := NewLockerManager(4, 1*time.Second)
	wg := sync.WaitGroup{}
	wg.Add(processNum * len(test))
	for i := 0; i < processNum; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			for _, tt := range test {
				l, err := lm.Lock(tt.key)
				if err != nil {
					t.Errorf("Lock() err: %v", err)
				}
				err = l.Unlock()
				if err != nil {
					t.Errorf("Unlock() err: %v", err)
				}
			}
			wg.Done()
		})
	}
	wg.Wait()
	n := lm.Clean()
	if n != len(test) {
		t.Errorf("TestGetNode() pageId: got = %v, want = %v", n, len(test))
	}
}
