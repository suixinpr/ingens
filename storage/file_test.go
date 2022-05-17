package storage

import (
	"bytes"
	. "github/suixinpr/ingens/base"
	"testing"
)

func TestWriteAndRead(t *testing.T) {
	file, err := Open("./", "TestWriteAndRead")
	if err != nil {
		t.Errorf("WriteAndRead() Open error: %v", err)
		return
	}

	test := []struct {
		name string

		off int64
		buf []byte
	}{
		{"page: 0", int64(PageSize) * 0, []byte("page: 0")},
		{"page: 1", int64(PageSize) * 1, []byte("page: 1")},
		{"page: 2", int64(PageSize) * 2, []byte("page: 2")},
		{"page: 10", int64(PageSize) * 10, []byte("page: 10")},
		{"page: 5", int64(PageSize) * 5, []byte("page: 5")},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			err := Write(file, tt.off, tt.buf)
			if err != nil {
				t.Errorf("InsertEntry() Witre error: %v", err)
			}
			buf, err := Read(file, tt.off, len(tt.buf))
			if err != nil {
				t.Errorf("InsertEntry() Read error: %v", err)
			}
			if bytes.Compare(buf, tt.buf) != 0 {
				t.Errorf("InsertEntry() WriteAndRead: got = %v, want = %v", buf, tt.buf)
			}
		})
	}
	err = Remove(file)
	if err != nil {
		t.Errorf("Remove() Remove error: %v", err)
	}
}
