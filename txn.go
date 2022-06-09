package ingens

import (
	"errors"
	"github/suixinpr/ingens/base"
	"sync"
)

var (
	// ErrTxIsClosed transcation is closed
	ErrTnxIsClosed = errors.New("ingens: transcation is closed")

	// ErrTxIsInvalid transcation is invalid
	ErrTnxIsInvalid = errors.New("ingens: transcation is invalid")
)

type Txn struct {
	ing *Ingens

	mu sync.Mutex

	tid      base.TransactionId
	snapshot base.TransactionId

	closed  bool
	invalid uint32
}

// Get get the value of key
func (txn *Txn) Get(key []byte) ([]byte, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return nil, ErrTnxIsClosed
	}

	// key
	ikey := key
	if txn.ing.opt.Copy {
		ikey = txn.ing.mmgr.Alloc(uint32(len(key)))
		copy(ikey, key)
		defer txn.ing.mmgr.Free(ikey)
	}

	// check if the key is valid
	if err := txn.ing.opt.CheckKey(ikey); err != nil {
		return nil, err
	}

	// get
	return txn.ing.get(txn.snapshot, ikey)
}

// Setnx set key to hold the value
func (txn *Txn) Setnx(key, value []byte) (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTnxIsClosed
	}

	// key, value
	ikey, ivalue := key, value
	if txn.ing.opt.Copy {
		ikey = txn.ing.mmgr.Alloc(uint32(len(key)))
		copy(ikey, key)
		defer txn.ing.mmgr.Free(ikey)

		ivalue = txn.ing.mmgr.Alloc(uint32(len(value)))
		copy(ivalue, value)
		defer txn.ing.mmgr.Free(ivalue)
	}

	// check if the key is valid
	if err := txn.ing.opt.CheckKey(ikey); err != nil {
		return err
	}

	// check if the value is valid
	if err := txn.ing.opt.CheckValue(ivalue); err != nil {
		return err
	}

	// setnx
	return txn.ing.setnx(ikey, ivalue)
}

func (txn *Txn) Delete(key []byte) (err error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTnxIsClosed
	}

	// key
	ikey := key
	if txn.ing.opt.Copy {
		ikey = txn.ing.mmgr.Alloc(uint32(len(key)))
		copy(ikey, key)
		defer txn.ing.mmgr.Free(ikey)
	}

	// check if the key is valid
	if err := txn.ing.opt.CheckKey(ikey); err != nil {
		return err
	}

	return txn.ing.delete(txn.tid, ikey)
}

func (txn *Txn) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTnxIsClosed
	}

	return nil
}

func (txn *Txn) Rollback() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return ErrTnxIsClosed
	}

	// pop UndoRecord
	// exec

	return nil
}
