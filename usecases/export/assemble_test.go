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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/export"
)

func TestAssembleNodeStatuses(t *testing.T) {
	startedAt := time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC)
	completedAt := startedAt.Add(5 * time.Second)

	const defaultHomePath = "/exports/test-export"

	type expectedShard struct {
		status     string
		objects    int64
		err        string
		skipReason string
	}

	tests := []struct {
		name              string
		meta              *ExportMetadata // nil → built from nodeAssignments with defaults
		homePath          string          // empty → defaultHomePath
		nodeAssignments   map[string]map[string][]string
		nodeStatuses      map[string]*NodeStatus
		wantStatus        string // also determines allTerminal: true for Success/Failed, false otherwise
		wantErrorContains []string
		wantTookInMs      int64
		wantShards        map[string]map[string]expectedShard
	}{
		{
			name: "all nodes success",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0"}},
				"node2": {"Article": {"shard1"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Success, CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard0": {Status: export.ShardSuccess, ObjectsExported: 100}},
					},
				},
				"node2": {
					NodeName: "node2", Status: export.Success, CompletedAt: completedAt.Add(time.Second),
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard1": {Status: export.ShardSuccess, ObjectsExported: 200}},
					},
				},
			},
			wantStatus:   string(export.Success),
			wantTookInMs: 6000,
			wantShards: map[string]map[string]expectedShard{
				"Article": {
					"shard0": {status: string(export.ShardSuccess), objects: 100},
					"shard1": {status: string(export.ShardSuccess), objects: 200},
				},
			},
		},
		{
			name: "one node failed",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0"}},
				"node2": {"Article": {"shard1"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Success, CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard0": {Status: export.ShardSuccess, ObjectsExported: 100}},
					},
				},
				"node2": {
					NodeName: "node2", Status: export.Failed, Error: "disk full", CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard1": {Status: export.ShardFailed, ObjectsExported: 50, Error: "disk full"}},
					},
				},
			},
			wantStatus:        string(export.Failed),
			wantErrorContains: []string{"node2", "disk full"},
			wantTookInMs:      5000,
			wantShards: map[string]map[string]expectedShard{
				"Article": {
					"shard0": {status: string(export.ShardSuccess)},
					"shard1": {status: string(export.ShardFailed), err: "disk full"},
				},
			},
		},
		{
			name: "one node still transferring",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0"}},
				"node2": {"Article": {"shard1"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Success, CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard0": {Status: export.ShardSuccess, ObjectsExported: 100}},
					},
				},
				"node2": {
					NodeName: "node2", Status: export.Transferring,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard1": {Status: export.ShardTransferring, ObjectsExported: 30}},
					},
				},
			},
			wantStatus: string(export.Transferring),
			wantShards: map[string]map[string]expectedShard{
				"Article": {
					"shard1": {status: string(export.ShardTransferring), objects: 30},
				},
			},
		},
		{
			name: "no shard progress reported uses effective status",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0", "shard1"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Failed, Error: "node crashed",
					ShardProgress: map[string]map[string]*ShardProgress{},
				},
			},
			wantStatus:        string(export.Failed),
			wantErrorContains: []string{"node1", "node crashed"},
			wantShards: map[string]map[string]expectedShard{
				"Article": {
					"shard0": {status: string(export.ShardFailed)},
					"shard1": {status: string(export.ShardFailed)},
				},
			},
		},
		{
			name: "completed shard on failed node stays completed",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0", "shard1"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Failed, Error: "shard1 failed", CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {
							"shard0": {Status: export.ShardSuccess, ObjectsExported: 500},
							"shard1": {Status: export.ShardFailed, ObjectsExported: 10, Error: "write error"},
						},
					},
				},
			},
			wantStatus:        string(export.Failed),
			wantErrorContains: []string{"node1", "shard1 failed"},
			wantTookInMs:      5000,
			wantShards: map[string]map[string]expectedShard{
				"Article": {
					"shard0": {status: string(export.ShardSuccess), objects: 500},
					"shard1": {status: string(export.ShardFailed), objects: 10, err: "write error"},
				},
			},
		},
		{
			name: "skipped shard preserved on successful node",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0", "shard1"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Success, CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {
							"shard0": {Status: export.ShardSuccess, ObjectsExported: 500},
							"shard1": {Status: export.ShardSkipped, SkipReason: "empty shard"},
						},
					},
				},
			},
			wantStatus:   string(export.Success),
			wantTookInMs: 5000,
			wantShards: map[string]map[string]expectedShard{
				"Article": {
					"shard0": {status: string(export.ShardSuccess), objects: 500},
					"shard1": {status: string(export.ShardSkipped), skipReason: "empty shard"},
				},
			},
		},
		{
			name: "multiple classes across nodes",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0"}, "Product": {"shard0"}},
				"node2": {"Article": {"shard1"}, "Product": {"shard1"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Success, CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard0": {Status: export.ShardSuccess, ObjectsExported: 100}},
						"Product": {"shard0": {Status: export.ShardSuccess, ObjectsExported: 50}},
					},
				},
				"node2": {
					NodeName: "node2", Status: export.Success, CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard1": {Status: export.ShardSuccess, ObjectsExported: 200}},
						"Product": {"shard1": {Status: export.ShardSuccess, ObjectsExported: 75}},
					},
				},
			},
			wantStatus:   string(export.Success),
			wantTookInMs: 5000,
			wantShards: map[string]map[string]expectedShard{
				"Article": {
					"shard0": {status: string(export.ShardSuccess), objects: 100},
					"shard1": {status: string(export.ShardSuccess), objects: 200},
				},
				"Product": {
					"shard0": {status: string(export.ShardSuccess), objects: 50},
					"shard1": {status: string(export.ShardSuccess), objects: 75},
				},
			},
		},
		{
			name: "single node all transferring",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Transferring,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard0": {Status: export.ShardTransferring, ObjectsExported: 42}},
					},
				},
			},
			wantStatus: string(export.Transferring),
			wantShards: map[string]map[string]expectedShard{
				"Article": {
					"shard0": {status: string(export.ShardTransferring), objects: 42},
				},
			},
		},
		{
			name: "metadata fields propagated to response",
			meta: &ExportMetadata{
				ID: "my-export-123", Backend: "gcs", Classes: []string{"Article", "Product"},
				NodeAssignments: map[string]map[string][]string{
					"node1": {"Article": {"shard0"}},
				},
				StartedAt: startedAt,
			},
			homePath: "/backups/gcs/my-export-123",
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Transferring,
					ShardProgress: map[string]map[string]*ShardProgress{},
				},
			},
			wantStatus: string(export.Transferring),
		},
		{
			name: "all nodes failed",
			nodeAssignments: map[string]map[string][]string{
				"node1": {"Article": {"shard0"}},
				"node2": {"Article": {"shard1"}},
			},
			nodeStatuses: map[string]*NodeStatus{
				"node1": {
					NodeName: "node1", Status: export.Failed, Error: "disk full", CompletedAt: completedAt,
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard0": {Status: export.ShardFailed, Error: "disk full"}},
					},
				},
				"node2": {
					NodeName: "node2", Status: export.Failed, Error: "timeout", CompletedAt: completedAt.Add(2 * time.Second),
					ShardProgress: map[string]map[string]*ShardProgress{
						"Article": {"shard1": {Status: export.ShardFailed, Error: "timeout"}},
					},
				},
			},
			wantStatus:        string(export.Failed),
			wantErrorContains: []string{"failed"},
			wantTookInMs:      7000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meta := tc.meta
			if meta == nil {
				// Derive Classes from NodeAssignments keys.
				classSet := map[string]struct{}{}
				for _, classShards := range tc.nodeAssignments {
					for className := range classShards {
						classSet[className] = struct{}{}
					}
				}
				classes := make([]string, 0, len(classSet))
				for c := range classSet {
					classes = append(classes, c)
				}
				meta = &ExportMetadata{
					ID:              "test-export",
					Backend:         "s3",
					Classes:         classes,
					NodeAssignments: tc.nodeAssignments,
					StartedAt:       startedAt,
				}
			}

			homePath := tc.homePath
			if homePath == "" {
				homePath = defaultHomePath
			}

			status, allTerminal := assembleNodeStatuses(meta, homePath, tc.nodeStatuses)

			wantTerminal := tc.wantStatus == string(export.Success) || tc.wantStatus == string(export.Failed)
			assert.Equal(t, wantTerminal, allTerminal)
			assert.Equal(t, tc.wantStatus, status.Status)
			assert.Equal(t, meta.ID, status.ID)
			assert.Equal(t, meta.Backend, status.Backend)
			assert.Equal(t, homePath, status.Path)

			for _, substr := range tc.wantErrorContains {
				assert.Contains(t, status.Error, substr)
			}
			if len(tc.wantErrorContains) == 0 {
				assert.Empty(t, status.Error)
			}

			assert.Equal(t, tc.wantTookInMs, status.TookInMs)

			for className, shards := range tc.wantShards {
				require.NotNil(t, status.ShardStatus[className], "missing class %s", className)
				for shardName, want := range shards {
					got, ok := status.ShardStatus[className][shardName]
					require.True(t, ok, "missing shard %s/%s", className, shardName)
					assert.Equal(t, want.status, got.Status, "shard %s/%s status", className, shardName)
					if want.objects != 0 {
						assert.Equal(t, want.objects, got.ObjectsExported, "shard %s/%s objects", className, shardName)
					}
					if want.err != "" {
						assert.Equal(t, want.err, got.Error, "shard %s/%s error", className, shardName)
					}
					if want.skipReason != "" {
						assert.Equal(t, want.skipReason, got.SkipReason, "shard %s/%s skipReason", className, shardName)
					}
				}
			}
		})
	}
}
