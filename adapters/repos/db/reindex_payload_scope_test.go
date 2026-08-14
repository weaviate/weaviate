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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecodeReindexTaskPayload walks every shape a payload reaches a gate
// in and pins the scope each one produces. The three answers are not
// interchangeable: one closes a shard, one closes a collection, one
// closes the cluster, and picking the narrow one for a payload that does
// not support it lets a backup capture a shard mid-migration.
func TestDecodeReindexTaskPayload(t *testing.T) {
	tests := []struct {
		name           string
		payload        string
		wantScope      ReindexPayloadScope
		wantCollection string
		wantShards     []string
	}{
		{
			name: "collection and shards both decode",
			payload: `{"migrationType":"change-tokenization","collection":"Movies",
				"unitToShard":{"u1":"shardA","u2":"shardB"}}`,
			wantScope:      ReindexPayloadScopeShards,
			wantCollection: "Movies",
			wantShards:     []string{"shardA", "shardB"},
		},
		{
			name: "several units on one shard collapse to one entry",
			payload: `{"collection":"Movies",
				"unitToShard":{"u1":"shardA","u2":"shardA","u3":"shardB"}}`,
			wantScope:      ReindexPayloadScopeShards,
			wantCollection: "Movies",
			wantShards:     []string{"shardA", "shardB"},
		},
		{
			name:           "shard map absent leaves the collection as the narrowest scope",
			payload:        `{"migrationType":"change-tokenization","collection":"Movies"}`,
			wantScope:      ReindexPayloadScopeCollection,
			wantCollection: "Movies",
		},
		{
			name:           "shard map present but naming no shard",
			payload:        `{"collection":"Movies","unitToShard":{"u1":"","u2":""}}`,
			wantScope:      ReindexPayloadScopeCollection,
			wantCollection: "Movies",
		},
		{
			// The shape §13 calls "a field a newer node retyped": the
			// struct decode fails outright, the collection still reads.
			name:           "shard map retyped by a newer node",
			payload:        `{"collection":"Movies","unitToShard":"shardA"}`,
			wantScope:      ReindexPayloadScopeCollection,
			wantCollection: "Movies",
		},
		{
			// Decodes without error and leaves an empty collection, which
			// is the same loss as not decoding at all.
			name:      "collection field renamed by a newer node",
			payload:   `{"class":"Movies","unitToShard":{"u1":"shardA"}}`,
			wantScope: ReindexPayloadScopeCluster,
		},
		{name: "not json at all", payload: `not json`, wantScope: ReindexPayloadScopeCluster},
		{
			// The collection name is right there in the bytes. A scan
			// would recover it and scope the refusal to one collection,
			// leaving every other collection open to a backup that a
			// truncated payload gives no grounds to admit.
			name:      "truncated mid-payload with the name still visible",
			payload:   `{"collection":"Movies","unitToShard":{"u1":"sha`,
			wantScope: ReindexPayloadScopeCluster,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecodeReindexTaskPayload([]byte(tt.payload))
			require.Equal(t, tt.wantScope, got.Scope)
			require.Equal(t, tt.wantCollection, got.Collection)
			require.ElementsMatch(t, tt.wantShards, got.Shards)
			switch got.Scope {
			case ReindexPayloadScopeShards:
				require.NotEmpty(t, got.Collection)
				require.NotEmpty(t, got.Shards)
			case ReindexPayloadScopeCollection:
				require.NotEmpty(t, got.Collection)
				require.Empty(t, got.Shards,
					"a collection-wide scope must not carry a shard set a caller could narrow to")
			case ReindexPayloadScopeCluster:
				require.Empty(t, got.Collection,
					"a cluster-wide scope must not carry a collection a caller could narrow to")
				require.Empty(t, got.Shards)
			}
		})
	}
}
