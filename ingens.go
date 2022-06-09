package ingens

import (
	"errors"
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/manager/buffer"
	"github/suixinpr/ingens/manager/locker"
	"github/suixinpr/ingens/manager/memory"
	"github/suixinpr/ingens/manager/storage"
	"github/suixinpr/ingens/manager/transaction"
	"github/suixinpr/ingens/nodes"
	"github/suixinpr/ingens/undo"
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
	opt  *Option

	// btree
	file     *os.File
	meta     *meta // meta page 0
	root     base.PageNumber
	pageNum  base.PageNumber
	levelNum uint64
	levels   []base.PageNumber

	// manager
	bmgr *buffer.BufferManager
	mmgr *memory.MemoryManager
	lmgr *locker.LockerManager
	smgr *nodes.StorageManager
	tmgr *transaction.TransactionManager
	umgr *undo.UndoManager

	// close
	closed uint32
	closeT sync.WaitGroup // transaction
	closeB sync.WaitGroup // background
	closeC chan struct{}  // channel
}

// Open open database and return a Ingens instanse
func Open(path string, opt Option) (*Ingens, error) {
	var ing = &Ingens{closed: 0, opt: &opt}
	var err error

	if err := ing.opt.Check(); err != nil {
		return nil, err
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

	ing.closeB.Add(1)
	go ing.autoFlush()

	return ing, nil
}

// Close close the database
func (ing *Ingens) Close(wait bool) error {
	// set flag bit
	if !atomic.CompareAndSwapUint32(&ing.closed, 0, 1) {
		return ErrDatabaseIsClosed
	}

	// wait transaction
	if wait {
		ing.closeT.Wait()
	}

	// close channel
	close(ing.closeC)

	// wait background
	ing.closeB.Wait()

	return ing.file.Close()
}

// isClosed check if the database is closed
func (ing *Ingens) isClosed() bool {
	return atomic.LoadUint32(&ing.closed) == 1
}

// Begin begin a transation
func (ing *Ingens) Begin() (*Txn, error) {
	if ing.isClosed() {
		return nil, ErrDatabaseIsClosed
	}

	ing.closeT.Add(1)
	if ing.isClosed() {
		ing.closeT.Done()
		return nil, ErrDatabaseIsClosed
	}

	txn := &Txn{
		ing:      ing,
		tid:      base.InvalidTid,
		snapshot: ing.tmgr.GetSnapshot(),
	}

	return txn, nil
}

func (ing *Ingens) Exec(fn func(*Txn) error) error {
	return nil
}

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

	root := btree.EmptryPage(1, 0)
	err = btree.WritePage(ing.file, root)
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
	ing.btree.bufPool, err = buffer.NewBufferPool(2048, 256) // 2048 * 64KB = 128MB
	return err
}

func (ing *Ingens) autoFlush() {
	for {
		select {
		case <-time.After(time.Millisecond * 100):
			ing.btree.bufPool.Flush()
		case <-ing.closeC:
			ing.closeT.Wait()
			ing.btree.bufPool.Flush()
			ing.closeB.Done()
			return
		}
	}
}
