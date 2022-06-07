package ingens

import (
	"errors"
	"github/suixinpr/ingens/manager/memory"
	"time"
)

const (
	B   = 1
	KiB = 1024 * B
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

type Option struct {
	// entry
	KeySize   int
	ValueSize int
	Copy      bool

	// buffer manager
	BufferCapacity  uint64
	BufferBucketNum uint64

	// memory manager
	MinSize uint32
	MaxSize uint32

	// lock manager
	Timeout time.Duration

	// transaction manager
}

func DefaultOptions() Option {
	return Option{
		// entry
		KeySize:   1 * KiB,
		ValueSize: 1 * KiB,

		// buffer manager
		BufferCapacity:  2048, // 2048 * 64KB = 128MB
		BufferBucketNum: 256,

		// memory manager
		MinSize: 16 * B,
		MaxSize: 64 * KiB,

		// lock manager
		Timeout: 10 * time.Second,

		// transaction manager
	}
}

var (
	// errKeySizeTooLarge the key size is too large
	ErrKeySizeTooLarge = errors.New("ingens: the key size is too large")

	// errValueSizeTooLarge the value size is too large
	ErrValueSizeTooLarge = errors.New("ingens: the value size is too large")

	// ErrZeroBufferCapacity
	ErrZeroBufferCapacity = errors.New("ingens: bufferpool capacity cannot be zero")

	// ErrMemoryMinMaxSize MinSize of the memory manager cannot be greater than MaxSize
	ErrMemoryMinMaxSize = errors.New("ingens: MinSize of the memory manager cannot be greater than MaxSize")
)

const (
	// entry
	MaxKeySize   = 1 * KiB
	MaxValueSize = 1 * KiB
)

func (opt *Option) Check() error {
	if opt.KeySize > MaxKeySize {
		return ErrKeySizeTooLarge
	}

	if opt.ValueSize > MaxValueSize {
		return ErrValueSizeTooLarge
	}

	if opt.BufferCapacity == 0 {
		return ErrZeroBufferCapacity
	}

	if memory.AlignUpPowerOfTwo(opt.MinSize) > memory.AlignDownPowerOfTwo(opt.MaxSize) {
		return ErrMemoryMinMaxSize
	}

	return nil
}

var (
	// errKeyEmpty the value cannot be nil
	ErrKeyEmpty = errors.New("ingens: the key cannot be nil")

	// errKeyTooLarge the key is too large
	ErrKeyTooLarge = errors.New("ingens: the key is too large")

	// errValueEmpty the value cannot be nil
	ErrValueEmpty = errors.New("ingens: the value cannot be nil")

	// errValueTooLarge the value is too large
	ErrValueTooLarge = errors.New("ingens: the value is too large")
)

// CheckKey check if the key is valid
func (opt *Option) CheckKey(key []byte) error {
	if key == nil {
		return ErrKeyEmpty
	}

	if len(key) > opt.KeySize {
		return ErrKeyTooLarge
	}

	return nil
}

// CheckValue check if the value is valid
func (opt *Option) CheckValue(value []byte) error {
	if value == nil {
		return ErrValueEmpty
	}

	if len(value) > opt.ValueSize {
		return ErrValueTooLarge
	}

	return nil
}
