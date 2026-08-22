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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// tenantScalePayload builds the payload.mig a task on a multi-tenant collection
// writes: every tenant appears once in each of the three tenant-sized fields,
// which is what takes a real payload into the megabytes.
func tenantScalePayload(tb testing.TB, props []string, tenants int) []byte {
	tb.Helper()
	rec := reindexRecoveryRecord{
		TaskID:      "0193f0a2-3f4b-7c1e-9d2a-6f8b1c3d5e70",
		TaskVersion: 3,
		UnitID:      "0193f0a2-3f4b-7c1e-9d2a-6f8b1c3d5e71",
		Payload: ReindexTaskPayload{
			MigrationType: ReindexTypeEnableFilterable,
			Collection:    "Docs",
			Properties:    props,
			Tenants:       make([]string, 0, tenants),
			UnitToNode:    make(map[string]string, tenants),
			UnitToShard:   make(map[string]string, tenants),
		},
	}
	for i := 0; i < tenants; i++ {
		tenant := fmt.Sprintf("%08x-3f4b-7c1e-9d2a-6f8b1c3d5e70", i)
		rec.Payload.Tenants = append(rec.Payload.Tenants, tenant)
		rec.Payload.UnitToNode[tenant] = "node-1"
		rec.Payload.UnitToShard[tenant] = tenant
	}
	doc, err := json.Marshal(rec)
	require.NoError(tb, err)
	return doc
}

// multiPropTrackerProps is the property list of the tracker the probes below
// walk. A multi-property name cannot settle which property it belongs to
// ("a_b" is both one property and the list "a"+"b"), so every one of them has
// to open payload.mig.
var multiPropTrackerProps = []string{"a", "b", "c", "d"}

// ambiguousTrackerAt writes one such tracker dir, with no properties.mig —
// the population the parse bound and the shared memo are both for.
func ambiguousTrackerAt(tb testing.TB, lsm string, gen int, doc []byte) string {
	tb.Helper()
	name := migrationDirWithProps(MigrationDirPrefixEnableFilterable, multiPropTrackerProps) + genSuffix(gen)
	return writeTrackerWithPayload(tb, lsm, name, doc)
}

// writeTrackerWithPayload materializes one started-but-unfinished tracker dir
// carrying doc as its payload.mig, and no properties.mig.
func writeTrackerWithPayload(tb testing.TB, lsm, dirName string, doc []byte) string {
	tb.Helper()
	dir := filepath.Join(lsm, ".migrations", dirName)
	require.NoError(tb, os.MkdirAll(dir, 0o755))
	require.NoError(tb, os.WriteFile(filepath.Join(dir, reindexRecoveryPayloadFile), doc, 0o644))
	return dirName
}

// payloadTenantsUnderBound / payloadTenantsOverBound are tenant counts that put
// a tracker payload either side of [maxRecoveryPayloadBytes]; the sizes are
// asserted rather than assumed by [TestTenantScalePayloadStraddlesTheBound].
const (
	payloadTenantsUnderBound = 1_000
	payloadTenantsOverBound  = 10_000
)

func TestTenantScalePayloadStraddlesTheBound(t *testing.T) {
	require.Less(t, len(tenantScalePayload(t, multiPropTrackerProps, payloadTenantsUnderBound)),
		maxRecoveryPayloadBytes)
	require.Greater(t, len(tenantScalePayload(t, multiPropTrackerProps, payloadTenantsOverBound)),
		maxRecoveryPayloadBytes)
}

// TestOversizedTrackerPayloadIsRefusedNotParsed pins the bound itself: over it,
// the probe stats and stops, so the answer is the same "could not read this
// payload" a corrupt one produces, and the read counter stays honest.
func TestOversizedTrackerPayloadIsRefusedNotParsed(t *testing.T) {
	tests := []struct {
		name           string
		tenants        int
		wantOK         bool
		wantUnreadable bool
		wantReads      int
	}{
		{
			name:      "under the bound: parsed, and charged as a read",
			tenants:   payloadTenantsUnderBound,
			wantOK:    true,
			wantReads: 1,
		},
		{
			name:           "over the bound: refused, and charged as no read",
			tenants:        payloadTenantsOverBound,
			wantUnreadable: true,
			wantReads:      0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			name := ambiguousTrackerAt(t, lsm, 1, tenantScalePayload(t, multiPropTrackerProps, tc.tenants))
			migDir := filepath.Join(lsm, ".migrations", name)

			answer, readPayload := readTaskProps(migDir)
			require.Equal(t, tc.wantOK, answer.ok)
			require.Equal(t, tc.wantUnreadable, answer.unreadable)
			require.Equal(t, tc.wantReads == 1, readPayload)
			if tc.wantOK {
				require.Equal(t, multiPropTrackerProps, answer.props)
			}

			props := &taskPropsCache{}
			_, unreadable := migrationDirsOf(lsm, nil, "a", "filterable").
				cachingProps(props).inScopeFailingOpen(name)
			require.Equal(t, tc.wantUnreadable, unreadable,
				"the scope must report a refused payload the way it reports an unparseable one")
			require.Equal(t, tc.wantReads, props.count())
		})
	}
}

// TestOversizedTrackerPayloadKeepsTheDeleteSweepSafe pins what the DELETE
// handler concludes once the bound refuses a payload: it stops deleting the
// trackers only a payload could attribute, and keeps deleting the ones their
// own name proves. Under-deleting leaves the next re-enable to fail loudly on
// the migration record; over-deleting would lose another property's tracker.
func TestOversizedTrackerPayloadKeepsTheDeleteSweepSafe(t *testing.T) {
	ambiguous := migrationDirWithProps(MigrationDirPrefixEnableFilterable, multiPropTrackerProps) + genSuffix(1)
	unambiguous := migrationDirWithProps(MigrationDirPrefixEnableFilterable, []string{"cat"}) + genSuffix(1)

	tests := []struct {
		name         string
		dirName      string
		propName     string
		tenants      int
		wantSurvives bool
	}{
		{
			name:     "name settles it: deleted whatever the payload weighs",
			dirName:  unambiguous,
			propName: "cat",
			tenants:  payloadTenantsOverBound,
		},
		{
			name:     "payload settles it and fits: deleted",
			dirName:  ambiguous,
			propName: "a",
			tenants:  payloadTenantsUnderBound,
		},
		{
			name:         "payload settles it and is refused: left for the record check",
			dirName:      ambiguous,
			propName:     "a",
			tenants:      payloadTenantsOverBound,
			wantSurvives: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			writeTrackerWithPayload(t, lsm, tc.dirName,
				tenantScalePayload(t, multiPropTrackerProps, tc.tenants))
			logger, _ := test.NewNullLogger()

			cleanStaleMigrationDirsAt(t.Context(), lsm, tc.propName, "filterable", logger, &taskPropsCache{})

			dir := filepath.Join(lsm, ".migrations", tc.dirName)
			if tc.wantSurvives {
				require.DirExists(t, dir)
				return
			}
			require.NoDirExists(t, dir)
		})
	}
}

// TestOversizedTrackerPayloadMakesTheUnloadedGateHydrate pins the other
// caller's conclusion. The tracker belongs to a property literally called
// "a_b", so only its payload can say the sweep of "a" has nothing to do here —
// and a refused payload cannot, so the shard is hydrated rather than skipped.
func TestOversizedTrackerPayloadMakesTheUnloadedGateHydrate(t *testing.T) {
	dirName := migrationDirWithProps(MigrationDirPrefixEnableFilterable, []string{"a", "b"}) + genSuffix(1)

	tests := []struct {
		name      string
		tenants   int
		wantStale bool
	}{
		{name: "payload fits: it names another property, so the shard is skipped", tenants: payloadTenantsUnderBound},
		{name: "payload refused: the shard is hydrated instead", tenants: payloadTenantsOverBound, wantStale: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			writeTrackerWithPayload(t, lsm, dirName,
				tenantScalePayload(t, []string{"a_b"}, tc.tenants))
			logger, _ := test.NewNullLogger()

			stale, finalizable := hasStalePartialReindexState(lsm, "a", "filterable", nil, nil, logger)
			require.Equal(t, tc.wantStale, stale)
			require.False(t, finalizable, "the skip is !stale && !finalizable, so a row claiming a skip owes both")
		})
	}
}

// TestStaleMigrationDirCleanupStopsOnCancelledContext pins that a cancelled
// walk parses no further tracker payloads and removes nothing.
func TestStaleMigrationDirCleanupStopsOnCancelledContext(t *testing.T) {
	lsm := t.TempDir()
	name := ambiguousTrackerAt(t, lsm, 1,
		tenantScalePayload(t, multiPropTrackerProps, payloadTenantsUnderBound))
	logger, _ := test.NewNullLogger()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	props := &taskPropsCache{}
	committed := migrationCommittedStateOf(migrationRecordsAt(lsm, logger))
	scope := migrationDirsOf(lsm, nil, "a", "filterable").cachingProps(props).knownFrom(committed)
	err := cleanStaleMigrationDirsIn(ctx, scope, committed, logger)

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, props.count(), "a cancelled walk must not parse a tracker payload")
	require.DirExists(t, filepath.Join(lsm, ".migrations", name),
		"a cancelled walk must not remove anything either")
}

// BenchmarkRecoveryPayloadParse measures what one sidecar-less tracker payload
// costs the probe that opens it, at the tenant counts a real migration reaches.
// The per-op byte rate is the number the parse bound is set against.
func BenchmarkRecoveryPayloadParse(b *testing.B) {
	for _, tenants := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("tenants=%d", tenants), func(b *testing.B) {
			lsm := b.TempDir()
			doc := tenantScalePayload(b, multiPropTrackerProps, tenants)
			name := ambiguousTrackerAt(b, lsm, 1, doc)
			migDir := filepath.Join(lsm, ".migrations", name)
			b.SetBytes(int64(len(doc)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				readTaskProps(migDir)
			}
		})
	}
}

// coldShardTree lays out `shards` unloaded shards, each carrying one ambiguous
// sidecar-less tracker for a `tenants`-sized migration, and returns their LSM
// paths plus one tracker payload's size on disk.
func coldShardTree(tb testing.TB, shards, tenants int) (lsmPaths []string, payloadBytes int) {
	tb.Helper()
	root := tb.TempDir()
	doc := tenantScalePayload(tb, multiPropTrackerProps, tenants)
	for i := 0; i < shards; i++ {
		lsm := filepath.Join(root, fmt.Sprintf("shard-%03d", i))
		require.NoError(tb, os.MkdirAll(lsm, 0o755))
		ambiguousTrackerAt(tb, lsm, 1, doc)
		lsmPaths = append(lsmPaths, lsm)
	}
	return lsmPaths, len(doc)
}

// sweepTupleGrid is the (property, index type) grid one terminal cleanup of a
// 4-property enable-filterable task runs, plus a property the tracker does not
// name. Every tuple but the last asks the same cold shards about the same
// tracker.
var (
	sweepTupleProps      = []string{"a", "b", "c", "d", "unrelated"}
	sweepTupleIndexTypes = []string{"filterable"}
)

// BenchmarkUnloadedSweepGateAcrossTuples measures the unloaded-shard gate over
// that grid: each shard's tracker payload is either parsed once for the run or
// once per tuple. The payloads fit under the parse bound, so what the two arms
// differ by is the memo, not the refusal.
func BenchmarkUnloadedSweepGateAcrossTuples(b *testing.B) {
	lsmPaths, payloadBytes := coldShardTree(b, 20, payloadTenantsUnderBound)
	logger, _ := test.NewNullLogger()
	for _, shareMemo := range []bool{false, true} {
		name := "memo-per-tuple"
		if shareMemo {
			name = "memo-per-run"
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(payloadBytes * len(lsmPaths)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dirs := &dirNamesCache{}
				for _, indexType := range sweepTupleIndexTypes {
					for _, propName := range sweepTupleProps {
						props := dirs.trackerProps()
						if !shareMemo {
							props = nil
						}
						for _, lsm := range lsmPaths {
							hasStalePartialReindexState(lsm, propName, indexType, dirs, props, logger)
						}
					}
				}
			}
		})
	}
}

// TestUnloadedSweepGateReadsEachShardsPayloadOncePerRun counts the payload
// parses one terminal cleanup's tuple grid costs. The tuples all ask the same
// cold shards, so the run pays per shard, not per shard per tuple.
func TestUnloadedSweepGateReadsEachShardsPayloadOncePerRun(t *testing.T) {
	const shards = 3
	lsmPaths, _ := coldShardTree(t, shards, payloadTenantsUnderBound)
	logger, _ := test.NewNullLogger()

	// How many tuples of the grid have to open a payload at all — the rest are
	// settled by the tracker's name, and would read nothing however the memo is
	// scoped. Without this, a memo that simply stopped reading would pass too.
	readingTuples := 0
	for _, propName := range sweepTupleProps {
		props := &taskPropsCache{}
		hasStalePartialReindexState(lsmPaths[0], propName, sweepTupleIndexTypes[0], nil, props, logger)
		if props.count() > 0 {
			readingTuples++
		}
	}
	require.Greater(t, readingTuples, 1,
		"the grid must hold more than one tuple that reads, or there is nothing to share")

	perTuple := 0
	unshared, shared := &dirNamesCache{}, &dirNamesCache{}
	for _, indexType := range sweepTupleIndexTypes {
		for _, propName := range sweepTupleProps {
			for _, lsm := range lsmPaths {
				// A memo per gate call is what the run cost before one was
				// threaded through it.
				own := &taskPropsCache{}
				hasStalePartialReindexState(lsm, propName, indexType, unshared, own, logger)
				perTuple += own.count()

				hasStalePartialReindexState(lsm, propName, indexType, shared, shared.trackerProps(), logger)
			}
		}
	}

	require.Equal(t, shards*readingTuples, perTuple,
		"a memo per gate call re-reads every shard's payload for every tuple that asks")
	require.Equal(t, shards, shared.trackerProps().count(),
		"a memo per run reads each shard's payload once, whatever the grid asks")
}
