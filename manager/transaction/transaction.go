package transaction

import (
	"github/suixinpr/ingens/base"
	"sync"
	"sync/atomic"
)

type (
	TransactionManager struct {
		tidStatus *tableTidToCsn

		latestTid    base.TransactionId
		latestCsn    base.CommitSequenceNumber
		snapshotPool sync.Pool
	}

	Snapshot struct {
		tid base.TransactionId
		csn base.CommitSequenceNumber
	}

	tableTidToCsn struct {
		new   func() []base.CommitSequenceNumber
		slice sync.Map
	}
)

func NewTransactionManager() *TransactionManager {
	return nil
}

func (tmgr *TransactionManager) GetSnapshot() *Snapshot {
	snapshot := tmgr.snapshotPool.Get().(*Snapshot)
	snapshot.tid = base.TransactionId(atomic.LoadUint64((*uint64)(&tmgr.latestTid)))
	snapshot.csn = base.CommitSequenceNumber(atomic.LoadUint64((*uint64)(&tmgr.latestCsn)))

	// tid 可能比 csn 的版本早，但是没有关系
	// 以 csn 为准，tid 只是减少查看对过老事务提交序列号的查询
	return snapshot
}

func (tmgr *TransactionManager) GetTransactionId() base.TransactionId {
	return base.TransactionId(atomic.AddUint64((*uint64)(&tmgr.latestTid), 1))
}

func (tmgr *TransactionManager) CheckVisibility(tid base.TransactionId, snapshot *Snapshot) bool {
	if tid < snapshot.tid {
		return true
	}

	csn := tmgr.tidStatus.load(tid)
	return csn < snapshot.csn
}

func (tmgr *TransactionManager) FinishTransaction(tid base.TransactionId, snapshot *Snapshot) {
	tmgr.tidStatus.store(tid, atomic.AddUint64((*uint64)(&tmgr.latestCsn), 1))
	tmgr.snapshotPool.Put(snapshot)
}

func (table *tableTidToCsn) store(tid base.TransactionId, csn uint64) {
	v, _ := table.slice.LoadOrStore(tid>>16, table.new)
	t := v.([]base.CommitSequenceNumber)
	atomic.StoreUint64((*uint64)(&t[tid&0xffff]), csn)
}

func (table *tableTidToCsn) load(tid base.TransactionId) base.CommitSequenceNumber {
	v, _ := table.slice.LoadOrStore(tid>>16, table.new)
	t := v.([]base.CommitSequenceNumber)
	return base.CommitSequenceNumber(atomic.LoadUint64((*uint64)(&t[tid&0xffff])))
}
