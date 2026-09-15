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

package storobj

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeDocIDBatchBucket visits found keys in reverse order, so a result that
// comes back in iteration order proves the loop orders it, not the bucket.
type fakeDocIDBatchBucket struct {
	objects map[uint64][]byte
	err     error
	calls   int
	maxKeys int
}

func (f *fakeDocIDBatchBucket) GetBySecondaryBatch(_ context.Context, _ int, keys [][]byte,
	visit func(i int, value []byte) error,
) error {
	f.calls++
	f.maxKeys = max(f.maxKeys, len(keys))
	if f.err != nil {
		return f.err
	}
	for i := len(keys) - 1; i >= 0; i-- {
		if object, ok := f.objects[binary.LittleEndian.Uint64(keys[i])]; ok {
			if err := visit(i, object); err != nil {
				return err
			}
		}
	}
	return nil
}

type sliceDocIDIterator struct {
	ids []uint64
	pos int
}

func (it *sliceDocIDIterator) Next() (uint64, bool) {
	if it.pos >= len(it.ids) {
		return 0, false
	}
	it.pos++
	return it.ids[it.pos-1], true
}

func (it *sliceDocIDIterator) Len() int { return len(it.ids) }

func idRange(from, to uint64) []uint64 {
	ids := make([]uint64, 0, to-from+1)
	for id := from; id <= to; id++ {
		ids = append(ids, id)
	}
	return ids
}

func TestReadObjectsByDocID(t *testing.T) {
	bucketErr := errors.New("bucket read failed")
	decodeErr := errors.New("decode failed")

	type testCase struct {
		name        string
		ids         []uint64
		missing     []uint64
		skip        []uint64
		limit       int
		bucketErr   error
		decodeFails bool
		cancel      bool
		want        []uint64
		wantMissing []uint64
		wantErr     error
		wantCalls   int
		wantMaxKeys int
	}
	cases := []testCase{
		{name: "no doc ids", limit: 10, want: []uint64{}},
		{name: "one doc id", ids: []uint64{1}, limit: 10, want: []uint64{1}, wantCalls: 1, wantMaxKeys: 1},
		{name: "results keep iteration order", ids: []uint64{5, 3, 9, 1}, limit: 10, want: []uint64{5, 3, 9, 1}, wantCalls: 1, wantMaxKeys: 4},
		{name: "duplicate doc ids resolve each time", ids: []uint64{4, 4, 4}, limit: 10, want: []uint64{4, 4, 4}, wantCalls: 1, wantMaxKeys: 3},
		{name: "limit stops the read", ids: idRange(1, 10), limit: 3, want: []uint64{1, 2, 3}, wantCalls: 1, wantMaxKeys: 3},
		{name: "limit zero reads nothing", ids: idRange(1, 3), limit: 0, want: nil},
		{name: "negative limit reads nothing", ids: idRange(1, 3), limit: -1, want: nil},
		{name: "rounds are sized by the doc id count when the limit is larger", ids: idRange(1, 3), limit: 1000, want: idRange(1, 3), wantCalls: 1, wantMaxKeys: 3},
		{
			name: "missing doc ids are decoded with nil after the call and do not count", ids: idRange(1, 6), missing: []uint64{2, 3}, limit: 3,
			want: []uint64{1, 4, 5}, wantMissing: []uint64{2, 3}, wantCalls: 2, wantMaxKeys: 3,
		},
		{name: "skipped doc ids do not count", ids: idRange(1, 3), skip: []uint64{1}, limit: 2, want: []uint64{2, 3}, wantCalls: 2, wantMaxKeys: 2},
		{name: "more doc ids than one bucket call", ids: idRange(1, 1203), limit: 2000, want: idRange(1, 1203), wantCalls: 3, wantMaxKeys: maxDocIDsPerBucketCall},
		{name: "bucket error propagates", ids: idRange(1, 3), limit: 10, bucketErr: bucketErr, wantErr: bucketErr, wantCalls: 1, wantMaxKeys: 3},
		{name: "decode error propagates", ids: idRange(1, 3), limit: 10, decodeFails: true, wantErr: decodeErr, wantCalls: 1, wantMaxKeys: 3},
		{name: "cancelled context", ids: idRange(1, 3), limit: 10, cancel: true, wantErr: context.Canceled},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket := &fakeDocIDBatchBucket{objects: map[uint64][]byte{}, err: tc.bucketErr}
			isMissing := map[uint64]bool{}
			for _, id := range tc.missing {
				isMissing[id] = true
			}
			for _, id := range tc.ids {
				if !isMissing[id] {
					bucket.objects[id] = fmt.Appendf(nil, "object-%d", id)
				}
			}
			skip := map[uint64]bool{}
			for _, id := range tc.skip {
				skip[id] = true
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}

			var gotMissing []uint64
			got, err := ReadObjectsByDocID(ctx, bucket, &sliceDocIDIterator{ids: tc.ids}, tc.limit,
				func(docID uint64, object []byte) (uint64, bool, error) {
					if object == nil {
						gotMissing = append(gotMissing, docID)
						return 0, false, nil
					}
					if tc.decodeFails {
						return 0, false, decodeErr
					}
					require.Equal(t, fmt.Sprintf("object-%d", docID), string(object))
					return docID, !skip[docID], nil
				})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
			require.Equal(t, tc.wantMissing, gotMissing)
			require.Equal(t, tc.wantCalls, bucket.calls)
			require.Equal(t, tc.wantMaxKeys, bucket.maxKeys)
		})
	}
}
