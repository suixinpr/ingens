package undo

import (
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/nodes"
)

type UndoManager struct {
}

func (umgr *UndoManager) NewUndoRecordPtr(entry nodes.DataEntry) base.UndoRecordPtr {
	return 0
}
