package ingens

import (
	"errors"
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/storage"
	"unsafe"
)

var (
	errChecksum = errors.New("checksum error")

	errMagic = errors.New("magic error")
)

const (
	// fnv "ingens"
	magic uint64 = 0xF1434F740C53863D

	// version
	version uint64 = 010

	metaSize = unsafe.Sizeof(meta{})
)

type meta struct {
	magic   uint64
	version uint64
	status  uint64
	tid     base.TransactionId
	root    base.PageNumber
	pageNum base.PageNumber
	level   []base.PageNumber
}

func (ing *Ingens) initMeta() error {
	buf, err := storage.Read(ing.file, 0, base.PageSize)
	if err != nil {
		return err
	}

	m := (*meta)(unsafe.Pointer(&buf[0]))
	if magic != m.magic {
		return nil
	}
	if version < m.version {
		return nil
	}

	ing.meta = m
	return nil
}
