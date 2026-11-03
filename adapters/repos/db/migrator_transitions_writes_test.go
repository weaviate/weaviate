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

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// A write accepted during the upload would be dropped with the local data.
func TestWritesRefusedWhileFreezing(t *testing.T) {
	const class = "FreezingWrites"
	ctx := context.Background()
	repl := &additional.ReplicationProperties{}
	object := func() *storobj.Object {
		return testObjWithTime(class, strfmt.UUID(uuid.NewString()), time.Now().UnixMilli())
	}

	shard, idx, _ := userReadOnlyIndex(t, class)
	name := shard.Name()
	preparedObject := object()
	prepared := idx.ReplicateObject(ctx, name, "prepared", preparedObject, 0)
	require.NoError(t, prepared.FirstError())

	writes := map[string]func() error{
		"put":          func() error { return idx.putObject(ctx, object(), repl, "", 0) },
		"incoming put": func() error { return idx.IncomingPutObject(ctx, name, object(), 0) },
		"prepare": func() error {
			resp := idx.ReplicateObject(ctx, name, "late", object(), 0)
			return resp.FirstError()
		},
		"commit prepared before the freeze": func() error {
			resp := idx.CommitReplication(ctx, name, "prepared").(replica.SimpleResponse)
			return resp.FirstError()
		},
	}
	errs := make(map[string]error, len(writes))
	h, freeze := freezeHarness(t, idx, name)
	h.cloud.upload = func(context.Context) error {
		for write, do := range writes {
			errs[write] = do()
		}
		return nil
	}

	worker := freeze(7)
	for write := range writes {
		require.Error(t, errs[write], write)
	}

	h.cluster.setRecords()
	h.migrator.transitions.finish(ctx, transitionKey{class, name}, worker)
	found, err := shard.Exists(ctx, preparedObject.ID())
	require.NoError(t, err)
	require.False(t, found, "a refused commit leaves no part of the object behind")
	require.NoError(t, idx.putObject(ctx, object(), repl, "", 0), "an aborted freeze takes writes again")
}

// A re-freeze can halt the shard before the ended attempt's cleanup runs.
func TestEndedAttemptKeepsTheNextAttemptsFence(t *testing.T) {
	const class = "RefreezeWrites"
	ctx := context.Background()
	shard, idx, _ := userReadOnlyIndex(t, class)
	name := shard.Name()
	put := func() error {
		object := testObjWithTime(class, strfmt.UUID(uuid.NewString()), time.Now().UnixMilli())
		return idx.putObject(ctx, object, &additional.ReplicationProperties{}, "", 0)
	}
	h, freeze := freezeHarness(t, idx, name)
	key := transitionKey{class, name}
	halts := underlyingShard(t, shard).haltForTransferCount.Load

	first := freeze(7)
	second := freeze(12)
	require.Equal(t, int64(2), halts())

	h.migrator.transitions.finish(ctx, key, first)
	require.Error(t, put(), "attempt 12 is still uploading")
	require.Equal(t, int64(1), halts(), "the ended attempt releases its own halt")

	h.cluster.setRecords()
	h.migrator.transitions.finish(ctx, key, second)
	require.NoError(t, put(), "an aborted re-freeze takes writes again")
	require.Zero(t, halts(), "compaction and async replication resume")
}

// The halt flushes prior writes, so one in flight must finish first or the upload leaves it out.
func TestFreezeWaitsOutWritesInFlight(t *testing.T) {
	const class = "FreezeInFlight"
	shard, idx, _ := userReadOnlyIndex(t, class)
	name := shard.Name()
	h, _ := freezeHarness(t, idx, name)
	record := freezeRecord(7)
	record.Class, record.Tenant = class, name
	h.cluster.setRecords(record)
	worker := &transitionWorker{attempt: 7, done: make(chan struct{})}

	idx.backupLock.RLock(name) // a write in flight
	errc := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := h.migrator.transitions.freeze(context.Background(), record, worker, false)
		errc <- err
	}, idx.logger)

	require.Never(t, worker.halted.Load, 200*time.Millisecond, 10*time.Millisecond, "the halt waits out the write")
	idx.backupLock.RUnlock(name)
	require.Eventually(t, worker.halted.Load, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, <-errc)
}

// freezeHarness's freeze uploads an attempt on idx and leaves it running on a HOT tenant.
func freezeHarness(t *testing.T, idx *Index, name string) (*transitionHarness, func(attempt uint64) *transitionWorker) {
	t.Helper()
	class := idx.Config.ClassName.String()
	h := newTransitionHarness(t)
	h.migrator.db = indexRegistry(idx)
	idx.schemaReader = tenantReader(t, class, name, h.cluster, func() *sharding.Physical {
		return &sharding.Physical{Status: models.TenantActivityStatusHOT}
	})
	return h, func(attempt uint64) *transitionWorker {
		t.Helper()
		record := freezeRecord(attempt)
		record.Class, record.Tenant = class, name
		h.cluster.setRecords(record)
		worker := &transitionWorker{attempt: attempt, done: make(chan struct{})}
		_, err := h.migrator.transitions.freeze(context.Background(), record, worker, false)
		require.NoError(t, err)
		close(worker.done)
		return worker
	}
}

// A load mid-transition would open files being moved and let in writes that go with the local data.
func TestShardLoadRefusedMidTransition(t *testing.T) {
	const class = "LoadMidTransition"
	ctx := context.Background()
	object := func() *storobj.Object {
		return testObjWithTime(class, strfmt.UUID(uuid.NewString()), time.Now().UnixMilli())
	}
	loads := map[string]func(idx *Index, name string) error{
		"write": func(idx *Index, _ string) error {
			return idx.putObject(ctx, object(), &additional.ReplicationProperties{}, "", 0)
		},
		"incoming write": func(idx *Index, name string) error {
			return idx.IncomingPutObject(ctx, name, object(), 0)
		},
		"activation": func(idx *Index, name string) error {
			return idx.LoadLocalShardForTenantActivation(ctx, name, false)
		},
	}
	for caller, load := range loads {
		for _, running := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s with a running transition %v", caller, running), func(t *testing.T) {
				shard, idx, _ := userReadOnlyIndex(t, class)
				name := shard.Name()
				idx.shards.LoadAndDelete(name)
				require.NoError(t, shard.Shutdown(ctx))
				transition := freezeRecord(7).Transition
				if !running {
					transition = nil
				}
				idx.schemaReader = transitionReader{SchemaReader: idx.schemaReader, shard: name, transition: transition}

				err := load(idx, name)

				if !running {
					require.NoError(t, err)
					return
				}
				require.ErrorIs(t, err, errTenantInTransition)
				require.Nil(t, idx.shards.Load(name), "the shard stays unloaded")
			})
		}
	}
}

// transitionReader shows transition as the shard's running transition.
type transitionReader struct {
	local.SchemaReader
	shard      string
	transition *sharding.Transition
}

func (r transitionReader) Read(class string, retry bool, read func(*models.Class, *sharding.State) error) error {
	return r.SchemaReader.Read(class, retry, func(c *models.Class, state *sharding.State) error {
		withTransition := state.DeepCopy()
		p := withTransition.Physical[r.shard]
		p.Transition = r.transition
		withTransition.Physical[r.shard] = p
		return read(c, &withTransition)
	})
}
