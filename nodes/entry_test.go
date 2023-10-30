package nodes

import (
	"bytes"
	. "github/suixinpr/ingens/base"
	"testing"
)

func TestIndexEntry(t *testing.T) {
	test := []struct {
		name string

		key      []byte
		oldValue PageNumber
		newValue PageNumber
	}{
		{"abcde -> 12345", []byte("abcde"), 12345, 54321},
	}

	for _, tt := range test {
		ie := FormIndexEntry(tt.key, tt.oldValue)
		if int(ie.KeySize()) != len(tt.key) {
			t.Errorf("FormIndexEntry() KeySize: got = %v, want = %v", int(ie.KeySize()), len(tt.key))
		}
		if bytes.Compare(ie.Key(), tt.key) != 0 {
			t.Errorf("FormIndexEntry() key: got = %v, want = %v", ie.Key(), tt.key)
		}
		if ie.Value() != tt.oldValue {
			t.Errorf("FormIndexEntry() oldValue: got = %v, want = %v", ie.Key(), tt.key)
		}

		ie.SetValue(54321)
		if int(ie.KeySize()) != len(tt.key) {
			t.Errorf("FormIndexEntry() KeySize: got = %v, want = %v", int(ie.KeySize()), len(tt.key))
		}
		if bytes.Compare(ie.Key(), tt.key) != 0 {
			t.Errorf("FormIndexEntry() key: got = %v, want = %v", ie.Key(), tt.key)
		}
		if ie.Value() != tt.newValue {
			t.Errorf("FormIndexEntry() newValue: got = %v, want = %v", ie.Key(), tt.key)
		}
	}
}

func TestDataEntry(t *testing.T) {
	test := []struct {
		name string

		key   []byte
		value []byte
	}{
		{"abcde -> 12345", []byte("abcde"), []byte("12345")},
	}

	for _, tt := range test {
		de := FormDataEntry(tt.key, tt.value)
		if int(de.KeySize()) != len(tt.key) {
			t.Errorf("FormDataEntry() KeySize: got = %v, want = %v", int(de.KeySize()), len(tt.key))
		}
		if bytes.Compare(de.Key(), tt.key) != 0 {
			t.Errorf("FormDataEntry() key: got = %v, want = %v", de.Key(), tt.key)
		}
		if int(de.ValueSize()) != len(tt.value) {
			t.Errorf("FormDataEntry() ValueSize: got = %v, want = %v", int(de.ValueSize()), len(tt.value))
		}
		if bytes.Compare(de.Value(), tt.value) != 0 {
			t.Errorf("FormDataEntry() Value: got = %v, want = %v", de.Value(), tt.value)
		}
	}
}
