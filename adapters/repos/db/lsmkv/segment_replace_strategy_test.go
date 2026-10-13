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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// A caller threading its buffer forward hands getBySecondary one grown to the
// largest record it has read, whose spare capacity holds that record's bytes.
// Both lengths a record carries are read back out of it and used to slice.
func TestSegmentGetBySecondaryReusedBuffer(t *testing.T) {
	widestValue := bytes.Repeat([]byte("F"), 512)
	hitValue := []byte("hit-value")

	// declares more primary-key bytes than it stores
	corruptPKRecord := replaceRecordDeclaring([]byte("corrupt"), []byte("corrupt-pk"), 7, 200)
	// a tombstone with no value behind it, declaring 64 bytes of one
	corruptTombstone := append([]byte{0x01}, make([]byte, 8)...)
	binary.LittleEndian.PutUint64(corruptTombstone[1:9], 64)

	seg := segmentWithSecondaryRecords(t, []secondaryRecord{
		{secKey: []byte("sec-widest"), record: replaceRecord(widestValue, []byte("widest-pk"))},
		{secKey: []byte("sec-hit"), record: replaceRecord(hitValue, []byte("hit-pk"))},
		{secKey: []byte("sec-corrupt-pk"), record: corruptPKRecord},
		{secKey: []byte("sec-deleted"), record: append([]byte{0x01}, make([]byte, 8)...)},
		{secKey: []byte("sec-corrupt-tombstone"), record: corruptTombstone},
	})

	tests := []struct {
		name           string
		secKey         []byte
		wantValue      []byte
		wantPrimaryKey []byte
		wantErr        error
		// the parse must fail rather than answer out of the buffer's spare bytes
		wantOverReadBlocked bool
	}{
		{
			name:           "hit",
			secKey:         []byte("sec-hit"),
			wantValue:      hitValue,
			wantPrimaryKey: []byte("hit-pk"),
		},
		{
			name:    "miss",
			secKey:  []byte("sec-absent"),
			wantErr: lsmkv.NotFound,
		},
		{
			name:    "tombstoned key",
			secKey:  []byte("sec-deleted"),
			wantErr: lsmkv.Deleted,
		},
		{
			name:                "record declaring a primary key past its end",
			secKey:              []byte("sec-corrupt-pk"),
			wantOverReadBlocked: true,
		},
		{
			name:                "tombstone declaring a value past its end",
			secKey:              []byte("sec-corrupt-tombstone"),
			wantOverReadBlocked: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, buffer, err := seg.getBySecondary(0, []byte("sec-widest"), nil)
			require.NoError(t, err)
			require.Greater(t, cap(buffer), len(corruptPKRecord),
				"the reused buffer must outsize the records under test")
			require.Equal(t, byte('F'), buffer[len(corruptPKRecord)],
				"the bytes past the records under test must be the widest record's value")
			bufferCap := cap(buffer)

			if tt.wantOverReadBlocked {
				require.Panics(t, func() { seg.getBySecondary(0, tt.secKey, buffer) },
					"a length past the record must not be answered out of the reused buffer")
				return
			}

			primaryKey, value, gotBuffer, err := seg.getBySecondary(0, tt.secKey, buffer)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantValue, value)
			require.Equal(t, tt.wantPrimaryKey, primaryKey)
			require.Equal(t, bufferCap, cap(gotBuffer),
				"the returned buffer must keep the capacity the caller grew")
		})
	}
}

type secondaryRecord struct {
	secKey []byte
	record []byte
}

// segmentWithSecondaryRecords returns a segment reading the given records from
// memory, indexed at secondary position 0 in the order given.
func segmentWithSecondaryRecords(t *testing.T, records []secondaryRecord) *segment {
	t.Helper()

	var contents []byte
	nodes := make(segmentindex.Nodes, 0, len(records))
	for _, r := range records {
		start := uint64(len(contents))
		contents = append(contents, r.record...)
		nodes = append(nodes, segmentindex.Node{Key: r.secKey, Start: start, End: uint64(len(contents))})
	}

	tree := segmentindex.NewBalanced(nodes)
	index, err := tree.MarshalBinary()
	require.NoError(t, err)

	return &segment{
		strategy:         segmentindex.StrategyReplace,
		readFromMemory:   true,
		contents:         contents,
		secondaryIndices: []diskIndex{segmentindex.NewDiskTree(index)},
	}
}

func replaceRecord(value, primaryKey []byte) []byte {
	return replaceRecordDeclaring(value, primaryKey, uint64(len(value)), uint32(len(primaryKey)))
}

// replaceRecordDeclaring takes the two header lengths rather than reading them
// off value and primaryKey, so a caller can build a torn record.
func replaceRecordDeclaring(value, primaryKey []byte, declaredValueLength uint64, declaredPKLength uint32) []byte {
	record := make([]byte, 9, 9+len(value)+4+len(primaryKey))
	binary.LittleEndian.PutUint64(record[1:9], declaredValueLength)
	record = append(record, value...)
	record = binary.LittleEndian.AppendUint32(record, declaredPKLength)
	return append(record, primaryKey...)
}
