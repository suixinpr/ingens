package ingens

import (
	"errors"
	. "github/suixinpr/ingens/base"
	"sync"
)

var (
	// ErrTxIsClosed transcation is closed
	ErrTxIsClosed = errors.New("ingens: transcation is closed")

	// ErrTxIsInvalid transcation is invalid
	ErrTxIsInvalid = errors.New("ingens: transcation is invalid")
)

type Tx struct {
	ing     *Ingens
	mu      sync.Mutex
	tid     TransactionId
	closed  uint32
	invalid uint32
}

func (ing *Ingens) Begin() (*Tx, error) {
	if ing.isClosed() {
		return nil, ErrDatabaseIsClosed
	}

	tx := &Tx{ing: ing}
	return tx, nil
}

// Get get the value of key
func (tx *Tx) Get(key []byte) (value []byte, err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	defer func() {
		if err != nil {
			tx.abort()
		}
	}()

	if tx.isClosed() {
		return nil, ErrTxIsClosed
	}

	if tx.isInvalid() {
		return nil, ErrTxIsInvalid
	}

	// check if the key is valid
	if err := tx.ing.CheckKey(key); err != nil {
		return nil, err
	}

	return tx.ing.btree.get(key)
}

// Setnx set key to hold the value
func (tx *Tx) Setnx(key, value []byte) (err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	defer func() {
		if err != nil {
			tx.abort()
		}
	}()

	if tx.isClosed() {
		return ErrTxIsClosed
	}

	if tx.isInvalid() {
		return ErrTxIsInvalid
	}

	// check if the key is valid
	if err := tx.ing.CheckKey(key); err != nil {
		return err
	}

	// check if the value is valid
	if err := tx.ing.CheckValue(value); err != nil {
		return err
	}

	// insert
	return tx.ing.btree.setnx(key, value)
}

func (tx *Tx) Delete(key []byte) (err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	defer func() {
		if err != nil {
			tx.abort()
		}
	}()

	if tx.isClosed() {
		return ErrTxIsClosed
	}

	if tx.isInvalid() {
		return ErrTxIsInvalid
	}

	// check if the key is valid
	if err := tx.ing.CheckKey(key); err != nil {
		return err
	}

	return tx.ing.btree.delete(key)
}

func (tx *Tx) isClosed() bool {
	return tx.closed == 1
}

func (tx *Tx) isInvalid() bool {
	return tx.invalid == 1
}

func (tx *Tx) close() {
	tx.closed = 1
}

func (tx *Tx) abort() {
	tx.invalid = 1
}
