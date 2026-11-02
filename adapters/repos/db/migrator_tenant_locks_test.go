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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	lockTestClass = "Abc"
	lockTestShard = "t1"
	lockTestNode  = "node1"
)

// newLockTestMigrator wires a stub index just enough for the shard-scoped entry
// points to reach their guard. reachIndex decides whether they get past their
// own argument checks: with it off they return before touching a shard, which
// is all the lock test needs and keeps them off the unwired remote path.
func newLockTestMigrator(t *testing.T, reachIndex bool) *Migrator {
	t.Helper()

	idx, _ := newDropTestIndex(t)
	idx.getSchema = &fakeSchemaGetter{}

	var shards []string
	owner := []string{lockTestNode}
	if reachIndex {
		shards = []string{lockTestShard}
	} else {
		owner = []string{"other-node"}
	}

	state := &sharding.State{Physical: map[string]sharding.Physical{
		lockTestShard: {Name: lockTestShard, BelongsToNodes: owner},
	}}
	state.SetLocalName(lockTestNode)

	reader := schemaUC.NewMockSchemaReader(t)
	reader.EXPECT().Shards(lockTestClass).Return(shards, nil).Maybe()
	reader.EXPECT().Read(lockTestClass, true, mock.Anything).RunAndReturn(
		func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			return read(nil, state)
		}).Maybe()
	idx.schemaReader = reader

	return newDropTestMigrator(idx, lockTestClass, nil)
}

// migratorOpsWithoutClassLock drives every entry point that no longer takes
// classLocks.
func migratorOpsWithoutClassLock(reachIndex bool) map[string]func(*Migrator) error {
	var creates []*schemaUC.CreateTenantPayload
	if reachIndex {
		creates = []*schemaUC.CreateTenantPayload{{Name: lockTestShard, Status: models.TenantActivityStatusHOT}}
	}

	ctx := context.Background()
	return map[string]func(*Migrator) error{
		"NewTenants": func(m *Migrator) error {
			return m.NewTenants(ctx, &models.Class{Class: lockTestClass}, creates)
		},
		"UpdateTenants": func(m *Migrator) error {
			return m.UpdateTenants(ctx, &models.Class{Class: lockTestClass}, nil, false)
		},
		"DeleteTenants": func(m *Migrator) error {
			return m.DeleteTenants(ctx, lockTestClass, nil)
		},
		"GetShardsStatus": func(m *Migrator) error {
			_, err := m.GetShardsStatus(ctx, lockTestClass, "")
			return err
		},
		"GetShardsQueueSize": func(m *Migrator) error {
			_, err := m.GetShardsQueueSize(ctx, lockTestClass, "")
			return err
		},
		"UpdateShardStatus": func(m *Migrator) error {
			return m.UpdateShardStatus(ctx, lockTestClass, lockTestShard, models.TenantActivityStatusHOT, 0)
		},
	}
}

// Dropping classLocks is only safe because the index refuses the work itself
// once it is closed: getOptInitLocalShard, initLocalShardWithForcedLoading and
// dropShards all check closeLock/closed, and UpdateTenants checks it inline.
// That is what the class lock was standing in for.
func TestMigratorWorkRefusesClosedIndex(t *testing.T) {
	for name, op := range migratorOpsWithoutClassLock(true) {
		t.Run(name, func(t *testing.T) {
			m := newLockTestMigrator(t, true)
			idx := m.db.GetIndex(lockTestClass)

			idx.closeLock.Lock()
			idx.closed = true
			idx.closeLock.Unlock()

			require.ErrorIs(t, op(m), errAlreadyShutdown)
		})
	}
}

// UpdateTenants used to hold idx.closeLock for reading across its whole body
// while the HOT branch reacquired it via LoadLocalShard.
func TestUpdateTenantsSurvivesCloseLockWriterDuringHotActivation(t *testing.T) {
	m := newLockTestMigrator(t, true)
	idx := m.db.GetIndex(lockTestClass)

	// Already loaded, so LoadLocalShard returns right after its closeLock
	// acquisition instead of initialising against this stub index.
	idx.shards.Store(lockTestShard, NewMockShardLike(t))

	// backupLock is the first thing the HOT worker takes, so holding it parks
	// the worker between the closed check and the nested closeLock.RLock.
	idx.backupLock.Lock(lockTestShard)

	done := make(chan struct{})
	go func() {
		_ = m.UpdateTenants(context.Background(), &models.Class{Class: lockTestClass},
			[]*schemaUC.UpdateTenantPayload{{Name: lockTestShard, Status: models.TenantActivityStatusHOT}}, false)
		close(done)
	}()

	// A build that holds closeLock across the body shows the read lock here
	// within microseconds, which is the state the writer below needs to find; a
	// correct build holds nothing and the poll just expires.
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if !idx.closeLock.TryLock() {
			break
		}
		idx.closeLock.Unlock()
		time.Sleep(time.Millisecond)
	}

	// Stands in for a teardown: blocks every RLock that arrives after it.
	writerDone := make(chan struct{})
	go func() {
		idx.closeLock.Lock()
		close(writerDone)
		idx.closeLock.Unlock()
	}()

	time.Sleep(50 * time.Millisecond) // let the writer enqueue
	idx.backupLock.Unlock(lockTestShard)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("UpdateTenants deadlocked: it held closeLock for reading while LoadLocalShard reacquired it behind a queued writer")
	}
	<-writerDone
}

// classLocks is exclusive and per class, so holding it here made creating a
// tenant block deleting an unrelated one — and made a shard-status poll block
// every schema apply for the collection — for as long as shard init took.
func TestMigratorWorkTakesNoClassLock(t *testing.T) {
	key := indexID(schema.ClassName(lockTestClass))

	for name, op := range migratorOpsWithoutClassLock(false) {
		t.Run(name, func(t *testing.T) {
			m := newLockTestMigrator(t, false)

			m.classLocks.Lock(key) // stands in for other class-scoped work
			defer m.classLocks.Unlock(key)

			done := make(chan struct{})
			go func() { _ = op(m); close(done) }()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s waited on the class lock; it must not take it", name)
			}
		})
	}
}
