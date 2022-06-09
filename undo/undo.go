package undo

import (
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/manager/buffer"
	"github/suixinpr/ingens/manager/storage"
	"github/suixinpr/ingens/nodes"
)

type UndoManager struct {
	smgr *storage.StorageManager
	bmgr *buffer.BufferManager
}

func NewUndoManager() *UndoManager {
	return nil
}

func (umgr *UndoManager) NewUndoRecordPtr(tid base.TransactionId, entry nodes.DataEntry) base.UndoRecordPtr {
	return 0
}

func (umgr *UndoManager) SearchInVersionChain(entry nodes.DataEntry, snapshot base.TransactionId) nodes.DataEntry {
	return nil
}
