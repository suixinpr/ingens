package transaction

import (
	"github/suixinpr/ingens/base"
	"sync"
	"sync/atomic"
)

type TransactionManager struct {
	activeTid sync.Map
	latestTid base.TransactionId
}

func NewTransactionManager() *TransactionManager {
	return nil
}

func (tmgr *TransactionManager) GetSnapshot() base.TransactionId {
	return base.TransactionId(atomic.LoadUint64((*uint64)(&tmgr.latestTid)))
}

func (tmgr *TransactionManager) GetTransactionId() base.TransactionId {
	return base.TransactionId(atomic.AddUint64((*uint64)(&tmgr.latestTid), 1))
}
