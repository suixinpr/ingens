package storage

import (
	"hash/fnv"
)

type StorageManager interface {
	EmptryData() any
	Read(any) error
	Write(any) error
}

func Sum64(buf []byte) uint64 {
	f := fnv.New64()
	f.Write(buf)
	return f.Sum64()
}
