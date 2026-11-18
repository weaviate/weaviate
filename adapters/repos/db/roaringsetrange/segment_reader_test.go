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

package roaringsetrange

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestSegmentReader(t *testing.T) {
	logger, _ := test.NewNullLogger()

	mem := NewMemtable(logger)
	mem.Insert(13, []uint64{113, 213}) // ...1101
	mem.Insert(5, []uint64{15, 25})    // ...0101
	mem.Insert(0, []uint64{10, 20})    // ...0000
	mem.Delete(20, []uint64{120, 220})

	type testCase struct {
		value       uint64
		operator    filters.Operator
		expectedAdd []uint64
		expectedDel []uint64
	}

	testCases := []testCase{
		{
			value:       0,
			operator:    filters.OperatorGreaterThanEqual,
			expectedAdd: []uint64{10, 20, 15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorGreaterThan,
			expectedAdd: []uint64{15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorLessThanEqual,
			expectedAdd: []uint64{10, 20},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorLessThan,
			expectedAdd: []uint64{},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorEqual,
			expectedAdd: []uint64{10, 20},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorNotEqual,
			expectedAdd: []uint64{15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},

		{
			value:       5,
			operator:    filters.OperatorGreaterThanEqual,
			expectedAdd: []uint64{15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorGreaterThan,
			expectedAdd: []uint64{113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorLessThanEqual,
			expectedAdd: []uint64{10, 20, 15, 25},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorLessThan,
			expectedAdd: []uint64{10, 20},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorEqual,
			expectedAdd: []uint64{15, 25},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorNotEqual,
			expectedAdd: []uint64{10, 20, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},

		{
			value:       13,
			operator:    filters.OperatorGreaterThanEqual,
			expectedAdd: []uint64{113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorGreaterThan,
			expectedAdd: []uint64{},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorLessThanEqual,
			expectedAdd: []uint64{10, 20, 15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorLessThan,
			expectedAdd: []uint64{10, 20, 15, 25},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorEqual,
			expectedAdd: []uint64{113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorNotEqual,
			expectedAdd: []uint64{10, 20, 15, 25},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},

		{
			value:       1,
			operator:    filters.OperatorGreaterThanEqual,
			expectedAdd: []uint64{15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       1,
			operator:    filters.OperatorGreaterThan,
			expectedAdd: []uint64{15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       1,
			operator:    filters.OperatorLessThanEqual,
			expectedAdd: []uint64{10, 20},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       1,
			operator:    filters.OperatorLessThan,
			expectedAdd: []uint64{10, 20},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       1,
			operator:    filters.OperatorEqual,
			expectedAdd: []uint64{},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       1,
			operator:    filters.OperatorNotEqual,
			expectedAdd: []uint64{10, 20, 15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},

		// a set bit above every stored value has only an empty gap layer
		{
			value:       1 << 40,
			operator:    filters.OperatorEqual,
			expectedAdd: []uint64{},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       1 << 40,
			operator:    filters.OperatorNotEqual,
			expectedAdd: []uint64{10, 20, 15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
	}

	memMax := NewMemtable(logger)
	memMax.Insert(0, []uint64{1})
	memMax.Insert(math.MaxUint64-1, []uint64{2})
	memMax.Insert(math.MaxUint64, []uint64{3})

	maxCases := []testCase{
		{value: math.MaxUint64 - 1, operator: filters.OperatorGreaterThanEqual, expectedAdd: []uint64{2, 3}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64 - 1, operator: filters.OperatorGreaterThan, expectedAdd: []uint64{3}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64 - 1, operator: filters.OperatorLessThanEqual, expectedAdd: []uint64{1, 2}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64 - 1, operator: filters.OperatorLessThan, expectedAdd: []uint64{1}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64 - 1, operator: filters.OperatorEqual, expectedAdd: []uint64{2}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64 - 1, operator: filters.OperatorNotEqual, expectedAdd: []uint64{1, 3}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64, operator: filters.OperatorGreaterThanEqual, expectedAdd: []uint64{3}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64, operator: filters.OperatorGreaterThan, expectedAdd: []uint64{}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64, operator: filters.OperatorLessThanEqual, expectedAdd: []uint64{1, 2, 3}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64, operator: filters.OperatorLessThan, expectedAdd: []uint64{1, 2}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64, operator: filters.OperatorEqual, expectedAdd: []uint64{3}, expectedDel: []uint64{1, 2, 3}},
		{value: math.MaxUint64, operator: filters.OperatorNotEqual, expectedAdd: []uint64{1, 2}, expectedDel: []uint64{1, 2, 3}},
	}

	memDeletionsOnly := NewMemtable(logger)
	memDeletionsOnly.Delete(7, []uint64{70})

	var emptyCases, deletionsOnlyCases []testCase
	for _, op := range segmentReaderOperators {
		emptyCases = append(emptyCases, testCase{value: 5, operator: op})
		deletionsOnlyCases = append(deletionsOnlyCases, testCase{value: 5, operator: op, expectedDel: []uint64{70}})
	}

	segments := []struct {
		name  string
		mem   *Memtable
		cases []testCase
	}{
		{name: "with deletions", mem: mem, cases: testCases},
		{name: "with max values", mem: memMax, cases: maxCases},
		{name: "empty", mem: NewMemtable(logger), cases: emptyCases},
		{name: "deletions only", mem: memDeletionsOnly, cases: deletionsOnlyCases},
	}

	for _, seg := range segments {
		reader := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(seg.mem)))
		for _, tc := range seg.cases {
			t.Run(fmt.Sprintf("%s, %s %d", seg.name, tc.operator.Name(), tc.value), func(t *testing.T) {
				bm, release, err := reader.Read(context.Background(), tc.value, tc.operator)
				assert.NoError(t, err)
				defer release()
				assert.ElementsMatch(t, bm.Additions.ToArray(), tc.expectedAdd)
				assert.ElementsMatch(t, bm.Deletions.ToArray(), tc.expectedDel)
				// CombinedReader merges into a read's result in place, so zeroing it
				// fails every later case if a read returned layer 0's view
				bm.Additions.AndConc(sroar.NewBitmap(), 1)
			})
		}
	}
}

// Pins a read taking what it needs from layer 0 only before the pread cursor reuses its buffer.
func TestSegmentReaderPread(t *testing.T) {
	logger, _ := test.NewNullLogger()

	mem := NewMemtable(logger)
	var values []uint64
	for docID := uint64(0); docID < 1000; docID++ {
		value := docID * 0x9E3779B97F4A7C15 // spreads values over all 64 bits
		mem.Insert(value, []uint64{docID})
		values = append(values, value)
	}
	mem.Delete(0, []uint64{5000, 5001})

	nodes := mem.Nodes()
	entries := make([]segmentEntry, len(nodes))
	for i, node := range nodes {
		entries[i] = segmentEntry{key: node.Key, additions: node.Additions.ToArray(), deletions: node.Deletions.ToArray()}
	}
	seg := createSegmentsFromEntries(t, entries)

	// a reader needs 2 buffers, since it consumes one layer while its producer
	// reads the next
	const bufferCount = 2
	newPreadCursor := func() *SegmentCursorPread {
		return NewSegmentCursorPread(bytes.NewReader(seg), int64(len(seg)), bufferCount)
	}

	_, first, ok := newPreadCursor().First()
	require.True(t, ok)
	firstBytes := bytes.Clone(first.Additions.ToBuffer())
	c := newPreadCursor()
	_, first, _ = c.First()
	for range bufferCount {
		c.Next()
	}
	require.NotEqual(t, firstBytes, first.Additions.ToBuffer(),
		"fixture must make Next() overwrite layer 0's buffer or it tests nothing")

	testValues := []uint64{0, 1, values[1], values[500], values[999], values[999] + 1, math.MaxUint64 - 1, math.MaxUint64}
	ctx := context.Background()
	for _, op := range segmentReaderOperators {
		for _, value := range testValues {
			t.Run(fmt.Sprintf("%s %d", op.Name(), value), func(t *testing.T) {
				expected, _, err := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(mem))).Read(ctx, value, op)
				require.NoError(t, err)

				bm, release, err := NewSegmentReader(NewGaplessSegmentCursor(newPreadCursor())).Read(ctx, value, op)
				require.NoError(t, err)
				defer release()
				assert.Equal(t, expected.Additions.ToArray(), bm.Additions.ToArray())
				assert.Equal(t, expected.Deletions.ToArray(), bm.Deletions.ToArray())
			})
		}
	}
}

// Pins a read cancelled mid-merge returning a partial result with no error.
func TestSegmentReaderCancelledMidMerge(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mem := NewMemtable(logger)
	mem.Insert(5, []uint64{1})
	mem.Insert(4, []uint64{2})
	mem.Insert(1, []uint64{3})

	for _, op := range segmentReaderOperators {
		t.Run(op.Name(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cursor := &cancelOnNextCursor{
				SegmentCursor: NewGaplessSegmentCursor(newFakeSegmentCursor(mem)),
				cancel:        cancel,
				cancelAt:      2,
			}

			_, release, err := (&SegmentReader{cursor: cursor}).Read(ctx, 5, op)
			defer release()
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

// cancelOnNextCursor cancels the read's ctx on its cancelAt-th Next().
type cancelOnNextCursor struct {
	SegmentCursor
	cancel   context.CancelFunc
	calls    int
	cancelAt int
}

func (c *cancelOnNextCursor) Next() (uint8, roaringset.BitmapLayer, bool) {
	c.calls++
	if c.calls == c.cancelAt {
		c.cancel()
	}
	return c.SegmentCursor.Next()
}

var segmentReaderOperators = []filters.Operator{
	filters.OperatorEqual,
	filters.OperatorNotEqual,
	filters.OperatorLessThan,
	filters.OperatorLessThanEqual,
	filters.OperatorGreaterThan,
	filters.OperatorGreaterThanEqual,
}

// rangeBenchData spans 32 containers. Dense ids give bitmap containers and
// every 40th id gives array containers. Wide values set all 64 bits, narrow only 20.
type rangeBenchData struct {
	name  string
	mem   *Memtable
	value uint64
}

func newRangeBenchData(tb testing.TB) []rangeBenchData {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	const maxDocID = 32 << 16

	var data []rangeBenchData
	for _, ids := range []struct {
		name string
		step uint64
	}{{"dense", 1}, {"sparse", 40}} {
		for _, vals := range []struct {
			name string
			mask uint64
		}{{"wide", math.MaxUint64}, {"narrow", 1<<20 - 1}} {
			rnd := rand.New(rand.NewPCG(1, 2))
			mem := NewMemtable(logger)
			for docID := uint64(0); docID < maxDocID; docID += ids.step {
				mem.Insert(rnd.Uint64()&vals.mask, []uint64{docID})
			}
			data = append(data, rangeBenchData{
				name:  ids.name + "_" + vals.name,
				mem:   mem,
				value: rnd.Uint64() & vals.mask,
			})
		}
	}
	return data
}

var rangeBenchOperators = []filters.Operator{
	filters.OperatorGreaterThanEqual,
	filters.OperatorLessThan,
	filters.OperatorEqual,
	filters.OperatorNotEqual,
}

func BenchmarkSegmentReader(b *testing.B) {
	ctx := context.Background()
	for _, data := range newRangeBenchData(b) {
		reader := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(data.mem)))
		for _, op := range rangeBenchOperators {
			b.Run(fmt.Sprintf("%s/%s", data.name, op.Name()), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_, release, err := reader.Read(ctx, data.value, op)
					if err != nil {
						b.Fatal(err)
					}
					release()
				}
			})
		}
	}
}
