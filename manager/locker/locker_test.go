package locker

import (
	"strconv"
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

	rm := NewLockerManager(4, 1*time.Second)
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			ok := rm.Lock(tt.key)
			if !ok {
				t.Errorf("Lock() err: %v", ok)
			}
			rm.Unlock(tt.key)
		})
	}
	n := rm.Clean()
	if n != len(test) {
		t.Errorf("Clean() pageId: got = %v, want = %v", n, len(test))
	}
}

func TestParallelLockAndUnlock(t *testing.T) {
	processNum := 1000
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

	rm := NewLockerManager(4, 1*time.Second)
	for i := 0; i < processNum; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			for _, tt := range test {
				ok := rm.Lock(tt.key)
				if !ok {
					t.Errorf("Lock() err: %v", ok)
				}
				rm.Unlock(tt.key)
			}
		})
	}
	t.Cleanup(func() {
		n := rm.Clean()
		if n != len(test) {
			t.Errorf("Clean() pageId: got = %v, want = %v", n, len(test))
		}
	})
}
