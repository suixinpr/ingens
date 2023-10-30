package memory

import (
	"testing"
)

func TestLogBaseTwo(t *testing.T) {
	test := []struct {
		name string

		x    uint32
		want int
	}{
		{"0", 0, 0},
		{"1", 1, 0},
		{"2", 2, 1},
		{"4", 4, 2},
		{"128", 128, 7},
		{"256", 256, 8},
		{"0x80000000", 0x80000000, 31},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			got := LogBaseTwo(tt.x)
			if got != tt.want {
				t.Errorf("LogBaseTwo() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestAlignDownPowerOfTwo(t *testing.T) {
	test := []struct {
		name string

		x    uint32
		want uint32
	}{
		{"0", 0, 0},
		{"1", 1, 1},
		{"2", 2, 2},
		{"10", 10, 8},
		{"129", 129, 128},
		{"256", 256, 256},
		{"0xffffffff", 0xffffffff, 0x80000000},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			got := AlignDownPowerOfTwo(tt.x)
			if got != tt.want {
				t.Errorf("AlignDownPowerOfTwo() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestAlignUpPowerOfTwo(t *testing.T) {
	test := []struct {
		name string

		x    uint32
		want uint32
	}{
		{"0", 0, 0},
		{"1", 1, 1},
		{"2", 2, 2},
		{"10", 10, 16},
		{"127", 127, 128},
		{"256", 256, 256},
		{"0xffffffff", 0xffffffff, 0},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			got := AlignUpPowerOfTwo(tt.x)
			if got != tt.want {
				t.Errorf("AlignUpPowerOfTwo() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestAllocAndFree(t *testing.T) {
	test := []struct {
		name string

		size uint32
		want int
	}{
		{"1", 1, 16},
		{"2", 2, 16},
		{"10", 10, 16},
		{"127", 127, 128},
		{"256", 256, 256},
		{"1 << 17", 1 << 17, 1 << 17},
	}

	mmgr := NewMemoryManager(1<<4, 1<<16)
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			mem := mmgr.Alloc(tt.size)
			if cap(mem) != tt.want {
				t.Errorf("Alloc() got = %v, want = %v", len(mem), tt.want)
			}
			mmgr.Free(mem)
		})
	}
}
