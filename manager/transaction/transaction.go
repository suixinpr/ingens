package transaction

import (
	"github/suixinpr/ingens/base"
	"sync"
	"sync/atomic"
)

type (
	UndoRecord struct {
		prev     base.UndoRecordPtr
		oldEntry []byte
		newEntry []byte
	}
)

type TransactionManager struct {
	activeTid sync.Map
	latestTid base.TransactionId
}

func NewTransactionManager() *TransactionManager {
	return nil
}

func (txnManager *TransactionManager) GetSnapshot() base.TransactionId {
	return base.TransactionId(atomic.LoadUint64((*uint64)(&txnManager.latestTid)))
}

func (txnManager *TransactionManager) GetTransactionId() base.TransactionId {
	return base.TransactionId(atomic.AddUint64((*uint64)(&txnManager.latestTid), 1))
}
