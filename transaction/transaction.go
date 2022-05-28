package transaction

import (
	. "github/suixinpr/ingens/base"
	"sync"
	"sync/atomic"
)

type TransactionManager struct {
	activeTid sync.Map
	latestTid TransactionId
}

func NewTransactionManager() *TransactionManager {
	return nil
}

func (txManager *TransactionManager) GetSnapshot() TransactionId {
	return TransactionId(atomic.LoadUint64((*uint64)(&txManager.latestTid)))
}

func (txManager *TransactionManager) GetTransactionId() TransactionId {
	return TransactionId(atomic.AddUint64((*uint64)(&txManager.latestTid), 1))
}
