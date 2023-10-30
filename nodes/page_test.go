package nodes

import (
	"bytes"
	. "github/suixinpr/ingens/base"
	"testing"
)

func TestEmptryPage(t *testing.T) {
	test := []struct {
		name string

		pageId PageNumber
		level  uint64
	}{
		{"root", 1, 0},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			p := EmptryPage(tt.pageId, tt.level)
			if p.GetPageId() != tt.pageId {
				t.Errorf("EmptryPage() pageId: got = %v, want = %v", p.GetPageId(), tt.pageId)
			}
			if p.GetLevel() != tt.level {
				t.Errorf("EmptryPage() level: got = %v, want = %v", p.GetLevel(), tt.level)
			}
		})
	}
}

func TestInsertEntry(t *testing.T) {
	test := []struct {
		name string

		level  uint64
		entrys []Entry
	}{
		// IndexEntry
		{"IndexEntry", 1,
			[]Entry{
				FormIndexEntry([]byte("nil"), 3),
				FormIndexEntry([]byte("IndexEntry"), 10),
				FormIndexEntry([]byte("RepeatedIndexEntry"), 18),
				FormIndexEntry([]byte("DataEntry"), 9),
				FormIndexEntry([]byte("RepeatedDataEntry"), 17),
			},
		},

		// DataEntry
		{"DataEntry", 0,
			[]Entry{
				FormDataEntry([]byte("nil"), []byte("3")),
				FormDataEntry([]byte("IndexEntry"), []byte("10")),
				FormDataEntry([]byte("RepeatedIndexEntry"), []byte("18")),
				FormDataEntry([]byte("DataEntry"), []byte("9")),
				FormDataEntry([]byte("RepeatedDataEntry"), []byte("17")),
			},
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			p := EmptryPage(1, tt.level)
			for _, entry := range tt.entrys {
				err := p.InsertEntry(entry)
				if err != nil {
					t.Errorf("InsertEntry() error : %v", err)
					return
				}
			}

			for _, entry := range tt.entrys {
				off, found := p.BinarySearch(entry.Key())
				if !found {
					t.Errorf("InsertEntry() found : got = %v, want = %v", found, true)
				}
				e := p.GetEntry(off)
				if bytes.Compare(e.Data(), entry.Data()) != 0 {
					t.Errorf("InsertEntry() GetEntry : got = %v, want = %v", e.Data(), entry.Data())
				}
			}
		})
	}
}

func TestInsertEntryError(t *testing.T) {
	test := []struct {
		name string

		level  uint64
		entrys []Entry
		err    error
	}{
		// nil
		{"nil", 0,
			[]Entry{
				nil,
			},
			errInvalidEntry,
		},

		// IndexEntry
		{"RepeatedIndexEntry", 1,
			[]Entry{
				FormIndexEntry([]byte("RepeatedIndexEntry"), 1),
				FormIndexEntry([]byte("RepeatedIndexEntry"), 1),
			},
			errRepeatedEntry,
		},

		{"ManyRepeatedIndexEntry", 1,
			[]Entry{
				FormIndexEntry([]byte("nil"), 3),
				FormIndexEntry([]byte("IndexEntry"), 10),
				FormIndexEntry([]byte("RepeatedIndexEntry"), 18),
				FormIndexEntry([]byte("DataEntry"), 9),
				FormIndexEntry([]byte("RepeatedDataEntry"), 17),
				FormIndexEntry([]byte("nil"), 3),
				FormIndexEntry([]byte("IndexEntry"), 10),
				FormIndexEntry([]byte("RepeatedIndexEntry"), 18),
				FormIndexEntry([]byte("DataEntry"), 9),
				FormIndexEntry([]byte("RepeatedDataEntry"), 17),
			},
			errRepeatedEntry,
		},

		{"RepeatedDataEntry", 0,
			[]Entry{
				FormDataEntry([]byte("RepeatedDataEntry"), []byte("0")),
				FormDataEntry([]byte("RepeatedDataEntry"), []byte("0")),
			},
			errRepeatedEntry,
		},

		{"ManyDataEntry", 0,
			[]Entry{
				FormDataEntry([]byte("nil"), []byte("3")),
				FormDataEntry([]byte("IndexEntry"), []byte("10")),
				FormDataEntry([]byte("RepeatedIndexEntry"), []byte("18")),
				FormDataEntry([]byte("DataEntry"), []byte("9")),
				FormDataEntry([]byte("RepeatedDataEntry"), []byte("17")),
				FormDataEntry([]byte("nil"), []byte("3")),
				FormDataEntry([]byte("IndexEntry"), []byte("10")),
				FormDataEntry([]byte("RepeatedIndexEntry"), []byte("18")),
				FormDataEntry([]byte("DataEntry"), []byte("9")),
				FormDataEntry([]byte("RepeatedDataEntry"), []byte("17")),
			},
			errRepeatedEntry,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			p := EmptryPage(1, tt.level)
			errOccurs := false
			for _, entry := range tt.entrys {
				err := p.InsertEntry(entry)
				if err != nil && err != tt.err {
					t.Errorf("InsertEntry() error : got = %v, want = %v", err, tt.err)
				}
				if err == tt.err {
					errOccurs = true
				}
			}
			if !errOccurs {
				t.Errorf("InsertEntry() error did not occur: %v", tt.err)
				return
			}
		})
	}
}

func TestFindSplitLoc(t *testing.T) {
	test := []struct {
		name string

		page       *Page
		entrys     []Entry
		insertLoc  OffsetNumber
		insertSize OffsetNumber

		splitLoc OffsetNumber
	}{
		{"IndexEntry-1", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("a"), 0), // 12 + 1 = 13
				FormIndexEntry([]byte("b"), 2), // 12 + 1 = 13
			}, 1, 13,
			1,
		},
		{"IndexEntry-2", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("a"), 1), // 12 + 1 = 13
				FormIndexEntry([]byte("b"), 2), // 12 + 1 = 13
				FormIndexEntry([]byte("c"), 3), // 12 + 1 = 13
			}, 0, 13,
			2,
		},
		{"IndexEntry-3", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("abcdefg"), 0), // 12 + 7 = 19
				FormIndexEntry([]byte("h"), 1),       // 12 + 1 = 13
			}, 2, 13,
			1,
		},
		{"IndexEntry-4", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("a"), 0),       // 12 + 1 = 13
				FormIndexEntry([]byte("bcdefgh"), 1), // 12 + 7 = 19
			}, 2, 13,
			2,
		},
		{"IndexEntry-5", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("abcdefg"), 0), // 12 + 7 = 19
				FormIndexEntry([]byte("h"), 2),       // 12 + 1 = 13
				FormIndexEntry([]byte("i"), 3),       // 12 + 1 = 13
				FormIndexEntry([]byte("j"), 4),       // 12 + 1 = 13
				FormIndexEntry([]byte("k"), 5),       // 12 + 1 = 13
				FormIndexEntry([]byte("l"), 6),       // 12 + 1 = 13
				FormIndexEntry([]byte("m"), 7),       // 12 + 1 = 13
				FormIndexEntry([]byte("n"), 8),       // 12 + 1 = 13
			}, 1, 13,
			4,
		},
		{"IndexEntry-6", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("a"), 0),       // 12 + 1 = 13
				FormIndexEntry([]byte("b"), 1),       // 12 + 1 = 13
				FormIndexEntry([]byte("c"), 2),       // 12 + 1 = 13
				FormIndexEntry([]byte("d"), 3),       // 12 + 1 = 13
				FormIndexEntry([]byte("e"), 4),       // 12 + 1 = 13
				FormIndexEntry([]byte("f"), 5),       // 12 + 1 = 13
				FormIndexEntry([]byte("g"), 6),       // 12 + 1 = 13
				FormIndexEntry([]byte("hijklmn"), 8), // 12 + 7 = 19
			}, 7, 13,
			5,
		},
		{"IndexEntry-7", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("abc"), 1),   // 12 + 3 = 15
				FormIndexEntry([]byte("defg"), 2),  // 12 + 4 = 16
				FormIndexEntry([]byte("hijkl"), 3), // 12 + 5 = 17
				FormIndexEntry([]byte("mnopq"), 4), // 12 + 5 = 17
				FormIndexEntry([]byte("rstu"), 5),  // 12 + 4 = 16
				FormIndexEntry([]byte("vwx"), 6),   // 12 + 3 = 15
				FormIndexEntry([]byte("yz"), 7),    // 12 + 2 = 14
			}, 0, 14,
			4,
		},
		{"IndexEntry-8", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("abcdefghijklmn"), 0), // 12 + 14 = 28
				FormIndexEntry([]byte("o"), 2),              // 12 + 1 = 13
			}, 1, 13,
			1,
		},
		{"IndexEntry-9", EmptryPage(1, 1),
			[]Entry{
				FormIndexEntry([]byte("a"), 0),              // 12 + 1 = 13
				FormIndexEntry([]byte("bcdefghijklmno"), 2), // 12 + 14 = 28
			}, 1, 13,
			2,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.page
			for _, entry := range tt.entrys {
				err := p.InsertEntry(entry)
				if err != nil {
					t.Errorf("findSplitLoc() error : %v", err)
				}
			}
			if p.offsetToArray(p.findSplitLoc(p.arrayToOffset(tt.insertLoc), tt.insertSize)) != tt.splitLoc {
				t.Errorf("findSplitLoc() splitLoc : got = %v, want = %v", p.offsetToArray(p.findSplitLoc(p.arrayToOffset(tt.insertLoc), tt.insertSize)), tt.splitLoc)
			}
		})
	}
}

func TestSplit(t *testing.T) {
	test := []struct {
		name string

		page   *Page
		pageId PageNumber
		entry1 Entry
		entry2 Entry
		entry3 Entry
	}{
		// non-leaf node
		{"IndexEntry-left", EmptryPage(1, 1), 2,
			FormIndexEntry([]byte("b"), 1),
			FormIndexEntry([]byte("d"), 2),
			FormIndexEntry([]byte("a"), 3),
		},
		{"IndexEntry-middle", EmptryPage(1, 1), 2,
			FormIndexEntry([]byte("b"), 1),
			FormIndexEntry([]byte("d"), 2),
			FormIndexEntry([]byte("c"), 3),
		},
		{"IndexEntry-right", EmptryPage(1, 1), 2,
			FormIndexEntry([]byte("b"), 1),
			FormIndexEntry([]byte("d"), 2),
			FormIndexEntry([]byte("e"), 3),
		},

		// leaf node
		{"DataEntry-left", EmptryPage(1, 0), 2,
			FormDataEntry([]byte("b"), []byte("1")),
			FormDataEntry([]byte("d"), []byte("2")),
			FormDataEntry([]byte("a"), []byte("3")),
		},
		{"DataEntry-middle", EmptryPage(1, 0), 2,
			FormDataEntry([]byte("b"), []byte("1")),
			FormDataEntry([]byte("d"), []byte("2")),
			FormDataEntry([]byte("c"), []byte("3")),
		},
		{"DataEntry-right", EmptryPage(1, 0), 2,
			FormDataEntry([]byte("b"), []byte("1")),
			FormDataEntry([]byte("d"), []byte("2")),
			FormDataEntry([]byte("e"), []byte("3")),
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.page
			p.InsertEntry(tt.entry1)
			p.InsertEntry(tt.entry2)
			rp, err := p.Split(tt.pageId, tt.entry3)
			if err != nil {
				t.Errorf("Split() error : %v", err)
			}
			if rp.GetPageId() != p.GetRight() {
				t.Errorf("Split() right : got = %v, want = %v", rp.GetPageId(), p.GetRight())
			}
			if rp.GetLeft() != p.GetPageId() {
				t.Errorf("Split() left : got = %v, want = %v", rp.GetLeft(), p.GetPageId())
			}
			var found1, found2 bool
			_, found1 = p.BinarySearch(tt.entry1.Key())
			_, found2 = rp.BinarySearch(tt.entry1.Key())
			if (found1 && found2) || (!found1 && !found2) {
				t.Errorf("Split() found : got = %v, %v", found1, found2)
			}
			_, found1 = p.BinarySearch(tt.entry2.Key())
			_, found2 = rp.BinarySearch(tt.entry2.Key())
			if (found1 && found2) || (!found1 && !found2) {
				t.Errorf("Split() found : got = %v, %v", found1, found2)
			}
			_, found1 = p.BinarySearch(tt.entry3.Key())
			_, found2 = rp.BinarySearch(tt.entry3.Key())
			if (found1 && found2) || (!found1 && !found2) {
				t.Errorf("Split() found : got = %v, %v", found1, found2)
			}
		})
	}
}
