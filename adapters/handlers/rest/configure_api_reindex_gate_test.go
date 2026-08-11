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

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// reindexGateTask builds a DTM task carrying a reindex payload.
func reindexGateTask(t *testing.T, id string, status distributedtask.TaskStatus,
	collection string, unitToShard map[string]string,
) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(db.ReindexTaskPayload{
		Collection:  collection,
		UnitToShard: unitToShard,
	})
	require.NoError(t, err)
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Namespace:      db.ReindexNamespace,
		Status:         status,
		Payload:        raw,
	}
}

// The gate's whole selectivity lives in this compare: it is what stops a
// migration on one shard from refusing backups of every other shard, and what
// stops a migration being missed because a sibling shard is idle. The db-side
// test can only reach it through an injected lookup, so it is pinned here,
// against the closure production actually installs.
func TestShardReindexActivityBuilderScopesByCollectionAndShard(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tasks := map[string][]*distributedtask.Task{
		db.ReindexNamespace: {
			reindexGateTask(t, "t1", distributedtask.TaskStatusStarted, "MyClass",
				map[string]string{"u1": "shard1"}),
			reindexGateTask(t, "t2", distributedtask.TaskStatusStarted, "OtherClass",
				map[string]string{"u1": "shard9"}),
			reindexGateTask(t, "t3", distributedtask.TaskStatusFinished, "MyClass",
				map[string]string{"u1": "shard7"}),
		},
	}

	lookup := newShardReindexActivityBuilder(context.Background(),
		func(context.Context) (map[string][]*distributedtask.Task, error) {
			return tasks, nil
		}, logger)(context.Background())

	tests := []struct {
		name       string
		collection string
		shard      string
		wantLive   bool
	}{
		{
			name:       "the tuple a live task names",
			collection: "MyClass", shard: "shard1", wantLive: true,
		},
		{
			name:       "right collection, other shard",
			collection: "MyClass", shard: "shard2",
			wantLive: false,
		},
		{
			name:       "other collection, same shard name",
			collection: "MyClass", shard: "shard9",
			wantLive: false,
		},
		{
			name:       "the other collection's own tuple",
			collection: "OtherClass", shard: "shard9", wantLive: true,
		},
		{
			name:       "a terminal task holds nothing",
			collection: "MyClass", shard: "shard7",
			wantLive: false,
		},
		{
			// The payload carries the collection as the submitter spelled it
			// and the caller as the schema spells it, so a case-sensitive
			// compare here would admit a backup of a held shard.
			name:       "the held tuple spelled with different case",
			collection: "myclass", shard: "shard1", wantLive: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantLive, lookup(tt.collection, tt.shard))
		})
	}
}

// A DTM the builder cannot reach must not read as "no migration anywhere":
// answering free from a question that was never asked admits a backup over a
// live migration.
func TestShardReindexActivityBuilderRefusesWhenDTMIsUnreachable(t *testing.T) {
	logger, hook := test.NewNullLogger()

	lookup := newShardReindexActivityBuilder(context.Background(),
		func(context.Context) (map[string][]*distributedtask.Task, error) {
			return nil, errors.New("raft: not leader")
		}, logger)(context.Background())

	assert.True(t, lookup("MyClass", "shard1"),
		"an unreachable DTM must refuse every backup, not clear them all")
	require.NotEmpty(t, hook.AllEntries(),
		"the operator has to be told why every backup is being refused")
}

// A live task whose payload will not decode names shards nobody can read, so
// none of them may read free. How far that spreads depends on how much of the
// payload survives, and it has to match what the commit-time backstop
// ([db.ReindexOverlapLookup]) does with the same payload — a wider admission
// gate refuses captures the commit would allow, a narrower one lets a capture
// upload in full before the commit rejects it.
func TestShardReindexActivityBuilderScopesUndecodablePayloads(t *testing.T) {
	brokenTask := func(t *testing.T, id, collection string, payload []byte) *distributedtask.Task {
		task := reindexGateTask(t, id, distributedtask.TaskStatusStarted, collection,
			map[string]string{"u1": "shard1"})
		task.Payload = payload
		return task
	}

	tests := []struct {
		name    string
		payload []byte
		// probes maps a (collection, shard) tuple to whether the gate must
		// report it held.
		probes map[[2]string]bool
		// decodesCleanly pins the shape of the payload itself: the renamed
		// case is only meaningful while json.Unmarshal accepts it, and a
		// future payload change that starts rejecting it would otherwise leave
		// the case passing for the wrong reason.
		decodesCleanly bool
	}{
		{
			// The rolling-upgrade case: a newer node retypes a field, the full
			// decoder gives up, the collection is still perfectly readable.
			name:    "a field retyped by a newer node",
			payload: []byte(`{"collection":"MyClass","unitToShard":"a-newer-node-changed-this-shape"}`),
			probes: map[[2]string]bool{
				{"MyClass", "shard1"}:      true,
				{"MyClass", "shard99"}:     true,
				{"myclass", "shard1"}:      true,
				{"OtherClass", "shard1"}:   false,
				{"UntouchedClass", "s42"}:  false,
				{"SiblingLiveClass", "sX"}: false,
			},
		},
		{
			// The other half of the rolling-upgrade shape, and the dangerous
			// one: a newer node RENAMES the collection field, Go ignores the
			// unknown key, and the payload decodes without error into an empty
			// collection. A gate that trusts the decoder's silence registers
			// the task under a collection no caller can name and reports every
			// shard free, while the commit-time backstop refuses the same
			// capture after all the upload work.
			name:           "the collection field renamed by a newer node",
			payload:        []byte(`{"collektion":"MyClass","unitToShard":{"u1":"shard1"}}`),
			decodesCleanly: true,
			probes: map[[2]string]bool{
				{"MyClass", "shard1"}:      true,
				{"OtherClass", "shard1"}:   true,
				{"UntouchedClass", "s42"}:  true,
				{"SiblingLiveClass", "sX"}: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var probe db.ReindexTaskPayload
			if tc.decodesCleanly {
				require.NoError(t, json.Unmarshal(tc.payload, &probe))
				require.Empty(t, probe.Collection)
			} else {
				require.Error(t, json.Unmarshal(tc.payload, &probe))
			}

			logger, hook := test.NewNullLogger()
			lookup := newShardReindexActivityBuilder(context.Background(),
				func(context.Context) (map[string][]*distributedtask.Task, error) {
					return map[string][]*distributedtask.Task{db.ReindexNamespace: {
						brokenTask(t, "t1", "MyClass", tc.payload),
						reindexGateTask(t, "t2", distributedtask.TaskStatusStarted,
							"SiblingLiveClass", map[string]string{"u1": "shardOK"}),
					}}, nil
				}, logger)(context.Background())

			for probe, want := range tc.probes {
				assert.Equalf(t, want, lookup(probe[0], probe[1]),
					"collection %q shard %q", probe[0], probe[1])
			}
			assert.True(t, lookup("SiblingLiveClass", "shardOK"),
				"a readable live task in the same snapshot still holds its own shards")
			require.NotEmpty(t, hook.AllEntries(),
				"the operator has to be told which backups are being refused and why")
		})
	}
}

// The restore gate's selectivity: a migration can run for days, so answering
// blind would refuse every restore in the cluster for its whole duration. The
// db-side test can only reach this through an injected lookup, so it is pinned
// here, against the closure production actually installs.
func TestAnyReindexActivityLookupScopesByCollection(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tasks := map[string][]*distributedtask.Task{
		db.ReindexNamespace: {
			reindexGateTask(t, "t1", distributedtask.TaskStatusStarted, "Logs",
				map[string]string{"u1": "shard1"}),
			reindexGateTask(t, "t2", distributedtask.TaskStatusFinished, "Archive",
				map[string]string{"u1": "shard7"}),
		},
	}
	lookup := newAnyReindexActivityLookup(
		func(context.Context) (map[string][]*distributedtask.Task, error) {
			return tasks, nil
		}, logger)

	tests := []struct {
		name        string
		collections []string
		wantLive    bool
	}{
		{
			name:        "a restore of an unrelated collection",
			collections: []string{"Docs"},
			wantLive:    false,
		},
		{
			name:        "a restore of the migrating collection",
			collections: []string{"Logs"},
			wantLive:    true,
		},
		{
			name:        "a restore that includes the migrating collection",
			collections: []string{"Docs", "Logs"},
			wantLive:    true,
		},
		{
			name:        "no class list yet, so the question is cluster-wide",
			collections: nil,
			wantLive:    true,
		},
		{
			name:        "a terminal task holds nothing",
			collections: []string{"Archive"},
			wantLive:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			live, err := lookup(context.Background(), tt.collections)
			require.NoError(t, err)
			assert.Equal(t, tt.wantLive, live)
		})
	}
}

// Same rule the backup half applies, on the same decoder: the refusal is held
// to the collection a broken payload still names, and only a payload naming no
// collection at all refuses every restore.
func TestAnyReindexActivityLookupScopesUndecodablePayloads(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		// probes maps a restore's collection list to whether it must be refused.
		probes map[string]bool
	}{
		{
			name:    "a field retyped by a newer node",
			payload: []byte(`{"collection":"Logs","unitToShard":"a-newer-node-changed-this-shape"}`),
			probes:  map[string]bool{"Logs": true, "logs": true, "Docs": false},
		},
		{
			name:    "the collection field renamed by a newer node",
			payload: []byte(`{"collektion":"Logs","unitToShard":{"u1":"shard1"}}`),
			probes:  map[string]bool{"Logs": true, "Docs": true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			broken := reindexGateTask(t, "t1", distributedtask.TaskStatusStarted, "Logs",
				map[string]string{"u1": "shard1"})
			broken.Payload = tc.payload
			lookup := newAnyReindexActivityLookup(
				func(context.Context) (map[string][]*distributedtask.Task, error) {
					return map[string][]*distributedtask.Task{db.ReindexNamespace: {broken}}, nil
				}, logger)

			for collection, want := range tc.probes {
				live, err := lookup(context.Background(), []string{collection})
				require.NoError(t, err)
				assert.Equalf(t, want, live, "restore of %q", collection)
			}
			live, err := lookup(context.Background(), nil)
			require.NoError(t, err)
			assert.True(t, live, "a restore with no class list yet must still be refused")
			require.NotEmpty(t, hook.AllEntries(),
				"the operator has to be told which restores are being refused and why")
		})
	}
}

// A DTM the lookup cannot reach must not read as "no migration anywhere": the
// error is what the gate turns into a refusal.
func TestAnyReindexActivityLookupFailsOnUnreachableDTM(t *testing.T) {
	logger, _ := test.NewNullLogger()
	lookup := newAnyReindexActivityLookup(
		func(context.Context) (map[string][]*distributedtask.Task, error) {
			return nil, errors.New("raft: not leader")
		}, logger)

	live, err := lookup(context.Background(), []string{"Docs"})
	require.Error(t, err)
	assert.False(t, live, "the refusal must come from the error, not from a made-up live task")
}
