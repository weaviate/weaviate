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

package export

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/export"
)

func TestDoExport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		classes  map[string][]shardSpec // className → shards to create
		mt       map[string]bool
		expected []expectedFile
	}{
		{
			name: "single class multiple shards",
			classes: map[string][]shardSpec{
				"Article": {
					{name: "shard0", numObjects: 300},
					{name: "shard1", numObjects: 200},
				},
			},
			expected: []expectedFile{
				{key: "Article_shard0_0000.parquet", numRows: 300},
				{key: "Article_shard1_0000.parquet", numRows: 200},
			},
		},
		{
			name: "multiple classes",
			classes: map[string][]shardSpec{
				"Article": {{name: "shard0", numObjects: 100}},
				"Product": {{name: "shard0", numObjects: 150}},
			},
			expected: []expectedFile{
				{key: "Article_shard0_0000.parquet", numRows: 100},
				{key: "Product_shard0_0000.parquet", numRows: 150},
			},
		},
		{
			name: "empty shard",
			classes: map[string][]shardSpec{
				"Article": {{name: "shard0", numObjects: 0}},
			},
			expected: nil, // no parquet file for empty shards
		},
		{
			name: "multi-tenant",
			classes: map[string][]shardSpec{
				"Article": {{name: "tenantA", numObjects: 50}},
			},
			mt: map[string]bool{"Article": true},
			expected: []expectedFile{
				{key: "Article_tenantA_0000.parquet", numRows: 50},
			},
		},
		{
			name: "single object",
			classes: map[string][]shardSpec{
				"Article": {{name: "shard0", numObjects: 1}},
			},
			expected: []expectedFile{
				{key: "Article_shard0_0000.parquet", numRows: 1},
			},
		},
		{
			name: "larger dataset",
			classes: map[string][]shardSpec{
				"Article": {{name: "shard0", numObjects: 2500}},
			},
			expected: []expectedFile{
				{key: "Article_shard0_0000.parquet", numRows: 2500},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, _ := test.NewNullLogger()
			backend := &fakeBackend{}

			// Build selector from class/shard specs.
			sel := &fakeSelector{
				shards:        make(map[string]map[string]*testShard),
				mt:            tc.mt,
				snapshotsRoot: t.TempDir(),
			}
			reqShards := make(map[string][]string)
			var classNames []string

			for className, specs := range tc.classes {
				classNames = append(classNames, className)
				sel.shards[className] = make(map[string]*testShard)
				for _, spec := range specs {
					store, _ := createTestStore(t, spec.numObjects)
					t.Cleanup(func() { store.Shutdown(context.Background()) })
					sel.shards[className][spec.name] = &testShard{store: store, name: spec.name}
					reqShards[className] = append(reqShards[className], spec.name)
				}
			}

			p := NewParticipant(sel, nil, logger, &fakeExportClient{}, &fakeNodeResolver{}, "node1")
			req := &ExportRequest{
				ID:       "test-export",
				Backend:  "fake",
				Classes:  classNames,
				Shards:   reqShards,
				Bucket:   "bucket",
				Path:     "path",
				NodeName: "node1",
			}

			snapshots, skipped, snapErr := p.snapshotAllShards(context.Background(), req)
			require.NoError(t, snapErr)

			err := p.doExport(context.Background(), backend, req, newTestNodeStatus(req.NodeName), snapshots, skipped)
			require.NoError(t, err)

			for _, ef := range tc.expected {
				data := backend.getWritten(ef.key)
				require.NotNil(t, data, "expected file %s to be written", ef.key)
				if ef.numRows > 0 {
					rows := readParquetRows(t, data)
					assert.Len(t, rows, ef.numRows, "file %s", ef.key)
					assertUniqueIDs(t, rows)
				}
			}

			statusData := backend.getWritten("node_node1_status.json")
			require.NotNil(t, statusData)
			var status NodeStatus
			require.NoError(t, json.Unmarshal(statusData, &status))
			assert.Equal(t, export.Success, status.Status)
		})
	}
}

func TestDoExport_SkippedShard(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	store0, _ := createTestStore(t, 100)
	defer store0.Shutdown(context.Background())

	selector := &fakeSelector{
		shards: map[string]map[string]*testShard{
			"Article": {
				"shard0": {store: store0, name: "shard0"},
			},
		},
		skipped: map[string]map[string]string{
			"Article": {
				"shard1": "tenant is COLD",
			},
		},
		snapshotsRoot: t.TempDir(),
	}

	p := NewParticipant(selector, nil, logger, &fakeExportClient{}, &fakeNodeResolver{}, "node1")
	req := &ExportRequest{
		ID:       "test-export",
		Backend:  "fake",
		Classes:  []string{"Article"},
		Shards:   map[string][]string{"Article": {"shard0", "shard1"}},
		Bucket:   "bucket",
		Path:     "path",
		NodeName: "node1",
	}

	snapshots, skipped, snapErr := p.snapshotAllShards(context.Background(), req)
	require.NoError(t, snapErr)

	err := p.doExport(context.Background(), backend, req, newTestNodeStatus(req.NodeName), snapshots, skipped)
	require.NoError(t, err)

	// Exported shard should have data.
	data0 := backend.getWritten("Article_shard0_0000.parquet")
	require.NotNil(t, data0)
	assert.Len(t, readParquetRows(t, data0), 100)

	// Skipped shard should not produce a file.
	assert.Nil(t, backend.getWritten("Article_shard1_0000.parquet"))

	// Status should show shard1 as skipped.
	statusData := backend.getWritten("node_node1_status.json")
	require.NotNil(t, statusData)
	var status NodeStatus
	require.NoError(t, json.Unmarshal(statusData, &status))
	assert.Equal(t, export.Success, status.Status)
	assert.Equal(t, export.ShardSkipped, status.ShardProgress["Article"]["shard1"].Status)
	assert.Equal(t, "tenant is COLD", status.ShardProgress["Article"]["shard1"].SkipReason)
}

func TestDoExport_ContextCanceled(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	store0, _ := createTestStore(t, 500)
	defer store0.Shutdown(context.Background())

	selector := &fakeSelector{
		shards: map[string]map[string]*testShard{
			"Article": {
				"shard0": {store: store0, name: "shard0"},
			},
		},
		snapshotsRoot: t.TempDir(),
	}

	p := NewParticipant(selector, nil, logger, &fakeExportClient{}, &fakeNodeResolver{}, "node1")
	req := &ExportRequest{
		ID:       "test-export",
		Backend:  "fake",
		Classes:  []string{"Article"},
		Shards:   map[string][]string{"Article": {"shard0"}},
		Bucket:   "bucket",
		Path:     "path",
		NodeName: "node1",
	}

	// Create snapshots with a valid context, then cancel before doExport.
	snapshots, skipped, snapErr := p.snapshotAllShards(context.Background(), req)
	require.NoError(t, snapErr)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	err := p.doExport(ctx, backend, req, newTestNodeStatus(req.NodeName), snapshots, skipped)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestSnapshotAllShards_Error verifies that errors from SnapshotShards
// propagate correctly through snapshotAllShards.
func TestSnapshotAllShards_Error(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	store0, _ := createTestStore(t, 100)
	defer store0.Shutdown(context.Background())

	// shard0 succeeds, shard1 has a nil store which causes SnapshotShards to
	// return an error.
	selector := &fakeSelector{
		shards: map[string]map[string]*testShard{
			"Article": {
				"shard0": {store: store0, name: "shard0"},
				"shard1": {store: nil, name: "shard1"},
			},
		},
		snapshotsRoot: t.TempDir(),
	}

	p := NewParticipant(selector, nil, logger, &fakeExportClient{}, &fakeNodeResolver{}, "node1")
	req := &ExportRequest{
		ID:       "test-export",
		Backend:  "fake",
		Classes:  []string{"Article"},
		Shards:   map[string][]string{"Article": {"shard0", "shard1"}},
		Bucket:   "bucket",
		Path:     "path",
		NodeName: "node1",
	}

	_, _, err := p.snapshotAllShards(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store not found")
}
