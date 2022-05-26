package ingens

import (
	"errors"
	. "github/suixinpr/ingens/base"
	"github/suixinpr/ingens/bufpage"
	"github/suixinpr/ingens/storage"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var (
	// ErrDatabaseIsClosed db is closed
	ErrDatabaseIsClosed = errors.New("ingens: db is closed")
)

type Ingens struct {
	// status
	path string

	opt  Option // option
	file *os.File

	rw   sync.RWMutex
	meta *meta // meta

	btree *btree // btree
	// redoLog *redoLog // redolog

	// close
	closed uint32
	closeW sync.WaitGroup
	closeC chan struct{}
}

// Open open database and return a Ingens instanse
func Open(path string, opt *Option) (*Ingens, error) {
	var ing = &Ingens{closed: 0}
	var err error

	// Set default options if no options are provided.
	if opt == nil {
		opt = DefaultOptions
	}

	// 打开数据库文件
	ing.file, err = storage.Open(path, "ingens.data")
	if err != nil {
		return nil, err
	}

	// meta 页面读取
	if info, err := ing.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		if err := ing.init(); err != nil {
			return nil, err
		}
	} else {
		if err := ing.initMeta(); err != nil {
			return nil, err
		}
	}

	// btree
	if err := ing.initBtree(); err != nil {
		return nil, err
	}

	//ing.closeW.Add(1)
	//go ing.autoFlush()

	return ing, nil
}

// Close close the database
func (ing *Ingens) Close() error {
	// set flag bit
	if !atomic.CompareAndSwapUint32(&ing.closed, 0, 1) {
		return ErrDatabaseIsClosed
	}

	// flush

	return ing.file.Close()
}

// isClosed check if the database is closed
func (ing *Ingens) isClosed() bool {
	return atomic.LoadUint32(&ing.closed) == 1
}

func (ing *Ingens) Exec() {}

func (ing *Ingens) init() error {
	// 初始化2个页面，分别为meta和root页面
	var err error

	buf := make([]byte, PageSize)
	meta := (*meta)(unsafe.Pointer(&buf[0]))
	meta.magic = magic
	meta.version = version
	meta.status = 0
	meta.tid = 0
	meta.root = 1
	err = storage.Write(ing.file, 0, buf)
	if err != nil {
		return err
	}

	root := bufpage.EmptryPage(1, 0)
	err = bufpage.WritePage(ing.file, root)
	if err != nil {
		return err
	}

	ing.meta = meta
	return nil
}

// 初始化
func (ing *Ingens) initBtree() error {
	var err error
	ing.btree = &btree{ing: ing}
	ing.btree.root = ing.meta.root
	ing.btree.pageNum = ing.meta.pageNum
	copy(ing.btree.levels,
		unsafe.Slice((*PageNumber)(unsafe.Pointer(uintptr(unsafe.Pointer(ing.meta))+uintptr(metaSize))), levelSize))
	ing.btree.bufPool, err = NewBufferPool(2048, 256) // 2048 * 64KB = 128MB
	return err
}

func (ing *Ingens) autoFlush() {
	for {
		select {
		case <-time.After(time.Millisecond * 100):
			ing.btree.bufPool.Flush()
		case <-ing.closeC:
			ing.btree.bufPool.Flush()
			ing.closeW.Done()
			return
		}
	}
}
