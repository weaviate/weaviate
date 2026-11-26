//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// TestCommitlogParserRoaringSetLegacyNode pins WAL replay of legacy
// CommitTypeRoaringSet records against SegmentNode's nil-bitmap accessor
// contract: a record whose additions or deletions region is empty
// (zero-length, e.g. a deletions-only write) decodes to a nil bitmap, and
// replay must consume it as empty rather than fail.
func TestCommitlogParserRoaringSetLegacyNode(t *testing.T) {
	tests := []struct {
		name      string
		additions []uint64
		deletions []uint64
	}{
		{name: "deletions only", deletions: []uint64{7, 8}},
		{name: "additions only", additions: []uint64{1, 2, 3}},
		{name: "additions and deletions", additions: []uint64{1, 2}, deletions: []uint64{3}},
		{name: "both empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := []byte("k1")
			sn, err := roaringset.NewSegmentNode(key,
				roaringset.NewBitmap(tt.additions...),
				roaringset.NewBitmap(tt.deletions...))
			require.NoError(t, err)

			var gotKey []byte
			var gotAdditions, gotDeletions []uint64
			prs := &commitlogParserRoaringSet{
				parser: &commitloggerParser{},
				consume: func(mt *Memtable, key []byte, additions, deletions []uint64) error {
					gotKey = key
					gotAdditions = additions
					gotDeletions = deletions
					return nil
				},
			}

			require.NoError(t, prs.parseNode(bytes.NewReader(sn.ToBuffer())))
			assert.Equal(t, key, gotKey)
			assert.Equal(t, tt.additions, gotAdditions)
			assert.Equal(t, tt.deletions, gotDeletions)
		})
	}
}

// recordReader keeps the last buffer the parser read into, so the test can
// overwrite the record after the parse.
type recordReader struct {
	*bytes.Reader
	dst []byte
}

func (r *recordReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.dst = p[:n]
	return n, err
}

// TestCommitlogParserRoaringSetCopiesKey pins that a replayed key is a copy
// rather than a window into the record, which the memtable would keep alive.
func TestCommitlogParserRoaringSetCopiesKey(t *testing.T) {
	docIDs := []uint64{1, 2, 3}
	wantKey := []byte("some-primary-key")

	tests := []struct {
		name   string
		record func(t *testing.T) []byte
		parse  func(prs *commitlogParserRoaringSet, r io.Reader) error
	}{
		{
			name: "list record",
			record: func(t *testing.T) []byte {
				n, err := roaringset.NewSegmentNodeList(wantKey, docIDs, nil)
				require.NoError(t, err)
				return n.ToBuffer()
			},
			parse: func(prs *commitlogParserRoaringSet, r io.Reader) error {
				return prs.parseNodeList(r)
			},
		},
		{
			name: "bitmap record",
			record: func(t *testing.T) []byte {
				n, err := roaringset.NewSegmentNode(wantKey,
					roaringset.NewBitmap(docIDs...), roaringset.NewBitmap())
				require.NoError(t, err)
				return n.ToBuffer()
			},
			parse: func(prs *commitlogParserRoaringSet, r io.Reader) error {
				return prs.parseNode(r)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotKey []byte
			prs := &commitlogParserRoaringSet{
				parser: &commitloggerParser{},
				consume: func(mt *Memtable, key []byte, _, _ []uint64) error {
					gotKey = key
					return nil
				},
			}

			reader := &recordReader{Reader: bytes.NewReader(tt.record(t))}
			require.NoError(t, tt.parse(prs, reader))

			// a key that still reads correctly after this is a copy
			for i := range reader.dst {
				reader.dst[i] = 0xff
			}
			require.Equal(t, wantKey, gotKey)
		})
	}
}

func le64(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }

func le32(v uint32) []byte { return binary.LittleEndian.AppendUint32(nil, v) }

// withTotalLen prefixes fields with a total length that matches them, so the
// record passes the outer length check and reaches the inner fields.
func withTotalLen(fields ...[]byte) []byte {
	body := bytes.Join(fields, nil)
	return append(le64(uint64(8+len(body))), body...)
}

// TestCommitlogParserRoaringSetCorruptRecord pins that a damaged record is an
// error and not a panic. A v0 record has no checksum, so every length in it
// comes straight from the file. Recovery logs the error and keeps what it read
// before the record; a panic instead fails the bucket, and with it the shard.
func TestCommitlogParserRoaringSetCorruptRecord(t *testing.T) {
	records := []struct {
		name   string
		record []byte
	}{
		{name: "total length 0", record: le64(0)},
		{name: "total length 7", record: le64(7)},
		{name: "total length 4, more bytes follow", record: append(le64(4), make([]byte, 32)...)},
		{name: "total length beyond int64", record: append(le64(1<<63+16), make([]byte, 32)...)},
		{name: "total length beyond the data", record: append(le64(1<<20), make([]byte, 32)...)},
		{name: "record ends before the additions length", record: withTotalLen(le32(0))},
		{name: "additions length beyond the record", record: withTotalLen(le64(1<<40), make([]byte, 8))},
		{name: "additions length max uint64", record: withTotalLen(le64(math.MaxUint64), make([]byte, 8))},
		{name: "deletions length beyond the record", record: withTotalLen(le64(0), le64(1<<40))},
		{name: "record ends before the key length", record: withTotalLen(le64(0), le64(0))},
		{name: "key length beyond the record", record: withTotalLen(le64(0), le64(0), le32(1<<20))},
		{name: "record ends after the first chunk", record: append(le64(maxRecordPrealloc+8+64), make([]byte, maxRecordPrealloc)...)},
	}
	parsers := []struct {
		name  string
		parse func(prs *commitlogParserRoaringSet, r io.Reader) error
	}{
		{name: "bitmap record", parse: func(prs *commitlogParserRoaringSet, r io.Reader) error { return prs.parseNode(r) }},
		{name: "list record", parse: func(prs *commitlogParserRoaringSet, r io.Reader) error { return prs.parseNodeList(r) }},
	}

	for _, p := range parsers {
		for _, rec := range records {
			t.Run(p.name+"/"+rec.name, func(t *testing.T) {
				consumed := false
				prs := &commitlogParserRoaringSet{
					parser: &commitloggerParser{},
					consume: func(*Memtable, []byte, []uint64, []uint64) error {
						consumed = true
						return nil
					},
				}

				var err error
				require.NotPanics(t, func() { err = p.parse(prs, bytes.NewReader(rec.record)) })
				require.Error(t, err)
				require.False(t, consumed, "a damaged record must not reach the memtable")
			})
		}
	}
}

// TestCommitlogParserRoaringSetLengthDoesNotSizeAllocation pins that a total
// length read from a damaged record does not decide how much memory replay
// allocates: only bytes actually present in the log do.
func TestCommitlogParserRoaringSetLengthDoesNotSizeAllocation(t *testing.T) {
	const claimed = 1 << 30
	record := append(le64(claimed), make([]byte, 64)...)

	for _, parse := range []func(prs *commitlogParserRoaringSet, r io.Reader) error{
		func(prs *commitlogParserRoaringSet, r io.Reader) error { return prs.parseNode(r) },
		func(prs *commitlogParserRoaringSet, r io.Reader) error { return prs.parseNodeList(r) },
	} {
		prs := &commitlogParserRoaringSet{parser: &commitloggerParser{}}

		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		err := parse(prs, bytes.NewReader(record))
		runtime.ReadMemStats(&after)

		require.Error(t, err)
		require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(claimed/16))
	}
}

// TestCommitlogParserRoaringSetLargeRecord pins that a valid record longer than
// maxRecordPrealloc is read in full over several chunks. A read loop that
// rejected it would end the replay at a healthy record.
func TestCommitlogParserRoaringSetLargeRecord(t *testing.T) {
	key := []byte("large-record-key")
	deletions := []uint64{5, 6, 7}

	// 4100 values per 65536-range exceed sroar's array container, so each range
	// serializes as an 8 KiB bitmap container.
	bitmapAdditions := make([]uint64, 0, 2100*4100)
	for hi := uint64(0); hi < 2100; hi++ {
		for lo := uint64(0); lo < 4100; lo++ {
			bitmapAdditions = append(bitmapAdditions, hi<<16|lo)
		}
	}
	listAdditions := make([]uint64, maxRecordPrealloc/8+1024)
	for i := range listAdditions {
		listAdditions[i] = uint64(i)
	}

	tests := []struct {
		name      string
		additions []uint64
		record    func(t *testing.T) []byte
		parse     func(prs *commitlogParserRoaringSet, r io.Reader) error
	}{
		{
			name:      "bitmap record",
			additions: bitmapAdditions,
			record: func(t *testing.T) []byte {
				n, err := roaringset.NewSegmentNode(key,
					roaringset.NewBitmap(bitmapAdditions...), roaringset.NewBitmap(deletions...))
				require.NoError(t, err)
				return n.ToBuffer()
			},
			parse: func(prs *commitlogParserRoaringSet, r io.Reader) error { return prs.parseNode(r) },
		},
		{
			name:      "list record",
			additions: listAdditions,
			record: func(t *testing.T) []byte {
				n, err := roaringset.NewSegmentNodeList(key, listAdditions, deletions)
				require.NoError(t, err)
				return n.ToBuffer()
			},
			parse: func(prs *commitlogParserRoaringSet, r io.Reader) error { return prs.parseNodeList(r) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := tt.record(t)
			require.Greater(t, len(record), maxRecordPrealloc+8, "record must need a second read chunk")

			var gotKey []byte
			var gotAdditions, gotDeletions []uint64
			prs := &commitlogParserRoaringSet{
				parser: &commitloggerParser{},
				consume: func(mt *Memtable, key []byte, additions, deletions []uint64) error {
					gotKey = key
					gotAdditions = additions
					gotDeletions = deletions
					return nil
				},
			}

			require.NoError(t, tt.parse(prs, bytes.NewReader(record)))
			assert.Equal(t, key, gotKey)
			assert.Equal(t, tt.additions, gotAdditions)
			assert.Equal(t, deletions, gotDeletions)
		})
	}
}
