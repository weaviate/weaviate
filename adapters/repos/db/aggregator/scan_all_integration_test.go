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

//go:build integrationTest

package aggregator

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	scanAllObjectCount = 400
	scanAllBulkLen     = 2 * 1024
	scanAllSegments    = 2
)

// TestScanAllLSM_DecodedValuesOutliveTheScan pins that a value the callback keeps
// stays valid. Both replace cursors parse each node into a buffer the next call
// overwrites, in either access mode, so a retained value has to be a copy.
func TestScanAllLSM_DecodedValuesOutliveTheScan(t *testing.T) {
	tests := []struct {
		name       string
		pread      bool
		properties *storobj.PropertyExtraction
	}{
		{
			name:  "pread, decode everything",
			pread: true,
		},
		{
			// The shape grouper.groupAll passes.
			name:  "pread, decode the scanned property only",
			pread: true,
			properties: &storobj.PropertyExtraction{
				PropertyPaths: [][]string{{fixtureBulkProp}},
			},
		},
		{
			name: "mmap, decode everything",
		},
		{
			name: "mmap, decode the scanned property only",
			properties: &storobj.PropertyExtraction{
				PropertyPaths: [][]string{{fixtureBulkProp}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store, want := objectsBucketFixture(t, ctx, tt.pread,
				scanAllObjectCount, scanAllBulkLen, scanAllSegments)

			scanned := 0
			got := map[uint64]string{}
			scan := func(_ context.Context, props *models.PropertySchema, docID uint64) error {
				asMap, ok := (*props).(map[string]interface{})
				require.True(t, ok)
				bulk, ok := asMap[fixtureBulkProp].(string)
				require.True(t, ok)
				scanned++
				got[docID] = bulk
				return nil
			}

			require.NoError(t, ScanAllLSM(ctx, store, scan, tt.properties))
			// got is keyed by doc ID, so this also rules out an object served twice
			require.Equal(t, scanAllObjectCount, scanned)
			require.Equal(t, want, got)
		})
	}
}

const (
	fixtureClassName = "AggregatorFixture"

	// A second property, so an object carries more than the value asserted on.
	fixtureValueProp = "value"

	// Bulk property that dominates each object's stored size.
	fixtureBulkProp = "bulk"
)

// objectsBucketFixture builds an objects bucket shaped like
// Shard.initObjectBucket and returns the bulk property it stored per doc ID, so
// a scan is checked against what was written. WithMinMMapSize(0) keeps every
// segment mmapped, so readFromMemory is !pread and pread is the mode in force.
func objectsBucketFixture(tb testing.TB, ctx context.Context, pread bool,
	count, bulkLen, segments int,
) (*lsmkv.Store, map[uint64]string) {
	tb.Helper()

	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { store.Shutdown(ctx) })

	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithKeepTombstones(true),
		lsmkv.WithCalcCountNetAdditions(true),
		lsmkv.WithClassName(fixtureClassName),
		lsmkv.WithPread(pread),
		lsmkv.WithMinMMapSize(0),
	)
	require.NoError(tb, err)

	b := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(tb, b)
	// FlushAndSwitch below drives flushing, so segments is the count asked for.
	b.SetMemtableThreshold(1 << 40)

	require.Zero(tb, count%segments,
		"count must divide evenly into segments for the segment-count assertion below")

	padding := strings.Repeat("x", bulkLen)
	perSegment := count / segments
	stored := make(map[uint64]string, count)
	for i := 0; i < count; i++ {
		docID := uint64(i)
		bulk := fmt.Sprintf("%d-%s", i, padding)
		stored[docID] = bulk
		obj := storobj.FromObject(&models.Object{
			Class: fixtureClassName,
			ID:    strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprint(i))).String()),
			Properties: map[string]interface{}{
				fixtureValueProp: float64(i),
				// The index prefix makes each value distinct, so one aliasing
				// the cursor's node buffer reads as another object's text.
				fixtureBulkProp: bulk,
			},
		}, nil, nil, nil)
		obj.DocID = docID

		value, err := obj.MarshalBinary()
		require.NoError(tb, err)
		require.Greater(tb, len(value), bulkLen)
		require.Less(tb, len(value), bulkLen+1024)

		key, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
		require.NoError(tb, err)

		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)

		require.NoError(tb, b.Put(key, value,
			lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))

		if (i+1)%perSegment == 0 && i+1 < count {
			require.NoError(tb, b.FlushAndSwitch())
		}
	}
	require.NoError(tb, b.FlushAndSwitch())

	// The memtable cursor serves whatever is left unflushed, reaching neither
	// the access mode nor the segment cursor under test.
	require.Equal(tb, segments,
		countSegmentFiles(tb, filepath.Join(dir, helpers.ObjectsBucketLSM)))

	return store, stored
}

func countSegmentFiles(tb testing.TB, bucketDir string) int {
	tb.Helper()

	entries, err := os.ReadDir(bucketDir)
	require.NoError(tb, err)

	count := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), "segment-") && filepath.Ext(e.Name()) == ".db" {
			count++
		}
	}
	return count
}
