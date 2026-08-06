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
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// multiCollectionBackupableFixture is backupableFixture generalized to several collections, so
// the per-collection log line can be counted independently of shard count.
func multiCollectionBackupableFixture(t *testing.T, node string, byCollection map[string][]string) *DB {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger, localNodeName: node}
	db.indices = map[string]*Index{}
	for collection, shards := range byCollection {
		physical := make(map[string]sharding.Physical, len(shards))
		for _, s := range shards {
			physical[s] = sharding.Physical{Name: s, BelongsToNodes: []string{node}}
		}
		shardState := &sharding.State{IndexID: collection, Physical: physical}
		reader := schemaUC.NewMockSchemaReader(t)
		reader.On("Read", collection, true, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			fn := args.Get(2).(func(*models.Class, *sharding.State) error)
			require.NoError(t, fn(&models.Class{Class: collection}, shardState))
		})
		getter := schemaUC.NewMockSchemaGetter(t)
		getter.On("NodeName").Return(node)
		idx := &Index{
			db:           db,
			Config:       IndexConfig{ClassName: schema.ClassName(collection)},
			schemaReader: reader,
			getSchema:    getter,
		}
		db.indices[indexID(schema.ClassName(collection))] = idx
	}
	return db
}

// TestReindexGateLogVolumeAcrossShardCounts measures the operator-facing log volume at
// 1, 5, 60 and 1000 blocked shards. The aggregate line count must be flat in
// shard count, the reported count exact, and the sample capped at a literal.
//
// "Operator-facing" is measured at INFO, which is what Weaviate runs at by
// default, not at WARN: a per-shard line demoted to INFO is still one entry per
// shard in every production log.
func TestReindexGateLogVolumeAcrossShardCounts(t *testing.T) {
	const (
		collection = "WideClass"
		node       = "weaviate-0"
	)
	const wantSampleCap = 10

	for _, shardCount := range []int{1, 5, 60, 1000} {
		t.Run(fmt.Sprintf("%d shards", shardCount), func(t *testing.T) {
			shards := make([]string, 0, shardCount)
			for i := range shardCount {
				shards = append(shards, fmt.Sprintf("s%04d", i))
			}
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			db := multiCollectionBackupableFixture(t, node, map[string][]string{collection: shards})
			db.logger = logger
			db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
				return func(string, string) bool { return true }
			})
			db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
				return func(string, string) ReindexHold { return ReindexHoldNone }
			})

			err := db.Backupable(context.Background(), []string{collection})
			require.Error(t, err)

			entries := hook.AllEntries()
			var aggregate, operatorVisible int
			var sample []string
			var reported int
			var aggregateLevel logrus.Level
			for _, e := range entries {
				if e.Level <= logrus.InfoLevel {
					operatorVisible++
				}
				if strings.Contains(e.Message, "are held by the reindex gate") {
					aggregate++
					aggregateLevel = e.Level
					sample, _ = e.Data["blocked_shards"].([]string)
					reported, _ = e.Data["blocked_shard_count"].(int)
				}
			}
			require.Equal(t, 1, aggregate, "exactly one aggregate operator line regardless of shard count")
			require.LessOrEqual(t, aggregateLevel, logrus.WarnLevel,
				"the aggregate line is the refusal an operator must see, so it stays at WARN or above")
			require.Equal(t, 1, operatorVisible,
				"volume at the default log level must be flat: got %d entries at INFO or above", operatorVisible)
			require.Equal(t, shardCount, reported, "the count must be exact")
			require.LessOrEqual(t, len(sample), wantSampleCap, "sample must be capped at %d", wantSampleCap)

			// The sample must be the first names in sorted order, not an
			// arbitrary map-order slice: "the first N" is what the message
			// claims, and only sorting makes repeated refusals diff cleanly.
			wantSample := append([]string(nil), shards...)
			sort.Strings(wantSample)
			if len(wantSample) > wantSampleCap {
				wantSample = wantSample[:wantSampleCap]
			}
			require.Equal(t, wantSample, sample, "sample must be the sorted prefix")

			// The response body stays one sentence too.
			require.Equal(t, 1, strings.Count(err.Error(), "\n")+1,
				"the body must carry the reason once, not once per shard")
		})
	}
}

// TestReindexGateLogVolumeIsPerCollection pins one line PER COLLECTION, emitted in
// sorted collection order.
func TestReindexGateLogVolumeIsPerCollection(t *testing.T) {
	const node = "weaviate-0"
	byCollection := map[string][]string{
		"Zebra":  {"z0", "z1", "z2"},
		"Alpha":  {"a0"},
		"Middle": {"m0", "m1"},
	}
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	db := multiCollectionBackupableFixture(t, node, byCollection)
	db.logger = logger
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) bool { return true }
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		return func(string, string) ReindexHold { return ReindexHoldNone }
	})

	require.Error(t, db.Backupable(context.Background(), []string{"Zebra", "Alpha", "Middle"}))

	var gotOrder []string
	counts := map[string]int{}
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "are held by the reindex gate") {
			c, _ := e.Data["collection"].(string)
			gotOrder = append(gotOrder, c)
			n, _ := e.Data["blocked_shard_count"].(int)
			counts[c] = n
		}
	}
	require.Equal(t, []string{"Alpha", "Middle", "Zebra"}, gotOrder,
		"one line per collection, in sorted order so repeated refusals diff cleanly")
	require.Equal(t, map[string]int{"Alpha": 1, "Middle": 2, "Zebra": 3}, counts)
}

// TestAppendUniqueGateErrKeepsFirstPerMessage pins that appendUniqueGateErr keeps the FIRST
// error per distinct message and drops later ones, in order.
func TestAppendUniqueGateErrKeepsFirstPerMessage(t *testing.T) {
	seen := map[string]struct{}{}
	var got []error

	a1 := fmt.Errorf("reason A")
	a2 := fmt.Errorf("reason A") // same text, different instance
	b1 := fmt.Errorf("reason B")
	a3 := fmt.Errorf("reason A")
	b2 := fmt.Errorf("reason B")

	for _, e := range []error{a1, a2, b1, a3, b2} {
		got = appendUniqueGateErr(seen, got, e)
	}

	require.Len(t, got, 2)
	require.Same(t, a1, got[0], "the FIRST instance per message is kept")
	require.Same(t, b1, got[1], "insertion order is preserved")
	require.Equal(t, map[string]struct{}{"reason A": {}, "reason B": {}}, seen,
		"the caller's seen map is mutated in place")
}

// TestBackupableWithheldErrorsReachTheOperator pins the other half of the withholding
// contract: when a gate refusal wins the response, the errors it displaces are
// withheld from the body but must still reach the operator's log.
func TestBackupableWithheldErrorsReachTheOperator(t *testing.T) {
	const (
		blocked = "BlockedClass"
		node    = "weaviate-0"
	)
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	db := multiCollectionBackupableFixture(t, node, map[string][]string{blocked: {"s1"}})
	db.logger = logger
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) bool { return true }
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		return func(string, string) ReindexHold { return ReindexHoldNone }
	})

	// "BrokenClass" fails its shard enumeration, so it contributes a non-gate
	// error — and that one names the local node, which is why it is withheld.
	addBrokenSchemaIndex(t, db, node, "BrokenClass")

	err := db.Backupable(context.Background(), []string{blocked, "BrokenClass"})
	require.Error(t, err)
	require.NotContains(t, err.Error(), node,
		"the gate refusal wins the body; a node-naming error is withheld from it")
	require.NotContains(t, err.Error(), "BrokenClass",
		"the gate refusal wins the body; the other error is withheld from it")

	var found string
	var foundLevel logrus.Level
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "also hit") {
			found = e.Message
			foundLevel = e.Level
		}
	}
	require.NotEmpty(t, found,
		"withheld from the response, not from the operator: the displaced error must be logged")
	// Level is part of the contract, not a detail: below INFO the line is
	// absent from a default-configured production log, which is the same as
	// withholding it from the operator.
	require.LessOrEqual(t, foundLevel, logrus.InfoLevel,
		"the displaced error must be logged at INFO or above, got %s", foundLevel)
	require.Contains(t, found, "BrokenClass", "the log must carry the withheld detail")
	require.Contains(t, found, "1 other error(s)")
}

// addBrokenSchemaIndex registers an index whose shard enumeration fails, which
// is the non-gate error that names the local node.
func addBrokenSchemaIndex(t *testing.T, db *DB, node, collection string) {
	t.Helper()
	reader := schemaUC.NewMockSchemaReader(t)
	reader.On("Read", collection, true, mock.Anything).Return(errors.New("schema read failed"))
	getter := schemaUC.NewMockSchemaGetter(t)
	getter.On("NodeName").Return(node).Maybe()
	db.indices[indexID(schema.ClassName(collection))] = &Index{
		db:           db,
		Config:       IndexConfig{ClassName: schema.ClassName(collection)},
		schemaReader: reader,
		getSchema:    getter,
	}
}

// A backup of ["Movies", "Movis"] submitted during a migration must report the
// typo now. Withholding it means the operator waits out the whole migration,
// retries, and only then learns the class name was wrong. The class-missing
// error names no node, so it can ride along in the response body.
func TestBackupableSurfacesMissingClassAlongsideGateRefusal(t *testing.T) {
	const (
		blocked = "BlockedClass"
		node    = "weaviate-0"
	)
	logger, _ := logrustest.NewNullLogger()
	db := multiCollectionBackupableFixture(t, node, map[string][]string{blocked: {"s1"}})
	db.logger = logger
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		return func(string, string) bool { return true }
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		return func(string, string) ReindexHold { return ReindexHoldNone }
	})

	err := db.Backupable(context.Background(), []string{blocked, "Movis"})
	require.Error(t, err)

	require.Contains(t, err.Error(), "class Movis doesn't exist",
		"the typo must be reported in the same round as the gate refusal")
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"riding along must not break the gate classification: the caller still answers 422")
	require.NotContains(t, err.Error(), node,
		"the ride-along error names no node; nothing may carry one into the body")
}
