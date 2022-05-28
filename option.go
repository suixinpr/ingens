package ingens

import (
	"errors"
	"time"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
)

type Option struct {
	// entry
	KeySize   int
	ValueSize int

	// buffer
	BufferCapacity  uint64
	BufferBucketNum uint64

	// scheduler
	Timeout time.Duration
}

func DefaultOptions() Option {
	return Option{
		// entry
		KeySize:   1 * KiB,
		ValueSize: 1 * KiB,

		//buffer
		BufferCapacity:  2048, // 2048 * 64KB = 128MB
		BufferBucketNum: 256,

		// scheduler
		Timeout: 10 * time.Second,
	}
}

var (
	// errKeySizeTooLarge the key size is too large
	ErrKeySizeTooLarge = errors.New("ingens: the key size is too large")

	// errValueSizeTooLarge the value size is too large
	ErrValueSizeTooLarge = errors.New("ingens: the value size is too large")
)

const (
	// entry
	MaxKeySize   = 1 * KiB
	MaxValueSize = 1 * KiB
)

func (opt *Option) CheckOption() error {
	if opt.KeySize > MaxKeySize {
		return ErrKeySizeTooLarge
	}

	if opt.ValueSize > MaxValueSize {
		return ErrValueSizeTooLarge
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
