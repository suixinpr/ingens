package ingens

import "errors"

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024

	maxKeySize = 1 * KiB
)

type Option struct {
	MaxKeySize   int
	MaxValueSize int
}

var DefaultOptions = &Option{
	MaxKeySize:   1 * KiB,
	MaxValueSize: 1 * KiB,
}

var (
	// errKeyEmpty the value cannot be nil
	errKeyEmpty = errors.New("ingens: the key cannot be nil")

	// errKeyTooLarge the key is too large
	errKeyTooLarge = errors.New("ingens: the key is too large")

	// errValueEmpty the value cannot be nil
	errValueEmpty = errors.New("ingens: the value cannot be nil")

	// errValueTooLarge the value is too large
	errValueTooLarge = errors.New("ingens: the value is too large")
)

// CheckKey check if the key is valid
func (ing *Ingens) CheckKey(key []byte) error {
	if key == nil {
		return errKeyEmpty
	}

	if len(key) > ing.opt.MaxKeySize {
		return errKeyTooLarge
	}

	return nil
}

// CheckValue check if the value is valid
func (ing *Ingens) CheckValue(value []byte) error {
	if value == nil {
		return errKeyEmpty
	}

	if len(value) > ing.opt.MaxValueSize {
		return errKeyTooLarge
	}

	return nil
}
