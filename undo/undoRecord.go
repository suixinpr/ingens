package undo

import (
	"github/suixinpr/ingens/base"
)

type (
	undoRecordHeader struct {
		prev base.UndoRecordPtr
		size base.OffsetNumber
	}

	UndoRecord []byte
)
