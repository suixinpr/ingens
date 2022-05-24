package undo

import (
	. "github/suixinpr/ingens/base"
)

type (
	UndoRecord struct {
		prev     UndoRecordPtr
		oldEntry []byte
		newEntry []byte
	}
)
