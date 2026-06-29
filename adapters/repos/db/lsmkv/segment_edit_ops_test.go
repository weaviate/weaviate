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
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEditOps(t *testing.T) *SegmentEditOps {
	t.Helper()
	s := newSegmentEditOps(t.TempDir(), nil)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func set(ids ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

func removeOp(targets ...string) OpDescriptor {
	return OpDescriptor{Type: "remove_target_vectors", Targets: targets, CreatedAt: 100}
}

func TestSegmentEditOps_RegisterOpIdempotent(t *testing.T) {
	s := newTestEditOps(t)

	require.NoError(t, s.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", Targets: []string{"foo"}, CreatedAt: 100}))
	// Re-register with a different descriptor: the original must be kept.
	require.NoError(t, s.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", Targets: []string{"bar"}, CreatedAt: 999}))

	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "op1", ops[0].ID)
	assert.Equal(t, []string{"foo"}, ops[0].Descriptor.Targets)
	assert.EqualValues(t, 100, ops[0].Descriptor.CreatedAt)
}

func TestSegmentEditOps_LoadOpsSortedByCreatedAt(t *testing.T) {
	s := newTestEditOps(t)

	require.NoError(t, s.RegisterOp("b", OpDescriptor{Type: "t", CreatedAt: 200}))
	require.NoError(t, s.RegisterOp("a", OpDescriptor{Type: "t", CreatedAt: 100}))
	require.NoError(t, s.RegisterOp("c", OpDescriptor{Type: "t", CreatedAt: 100}))

	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 3)
	// Sorted by CreatedAt, ties broken by ID: (a,100), (c,100), (b,200).
	assert.Equal(t, []string{"a", "c", "b"}, []string{ops[0].ID, ops[1].ID, ops[2].ID})
}

func TestSegmentEditOps_SnapshotSegmentsIdempotent(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))

	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))
	require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("boom")))
	// Re-snapshotting must not duplicate or reset accrued retry state.
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2", "seg3"}))

	pending, err := s.AllPending()
	require.NoError(t, err)
	require.Len(t, pending, 3)
	byID := map[string]PendingSegment{}
	for _, p := range pending {
		byID[p.SegmentID] = p
	}
	assert.Equal(t, 1, byID["seg1"].Attempts)
	assert.Equal(t, "boom", byID["seg1"].LastError)
	assert.Equal(t, 0, byID["seg2"].Attempts)
	assert.Equal(t, 0, byID["seg3"].Attempts)
}

func TestSegmentEditOps_PendingScopedPerOp(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.RegisterOp("op2", removeOp("bar")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))
	require.NoError(t, s.SnapshotSegments("op2", []string{"seg3"}))

	p1, err := s.Pending("op1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg1", "seg2"}, p1)

	p2, err := s.Pending("op2")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg3"}, p2)

	// Unknown op returns nothing, not an error.
	pNone, err := s.Pending("missing")
	require.NoError(t, err)
	assert.Empty(t, pNone)
}

func TestSegmentEditOps_MarkSegmentDone(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))

	require.NoError(t, s.MarkSegmentDone("op1", "seg1"))

	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg2"}, p)

	// Idempotent: marking an already-done (or unknown) segment is a no-op.
	require.NoError(t, s.MarkSegmentDone("op1", "seg1"))
	require.NoError(t, s.MarkSegmentDone("missing", "seg1"))
}

func TestSegmentEditOps_BumpAttemptThenQuarantine(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1"}))

	for range 3 {
		require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("disk full")))
	}

	pending, err := s.AllPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, 3, pending[0].Attempts)
	assert.Equal(t, "disk full", pending[0].LastError)

	require.NoError(t, s.Quarantine("op1", "seg1"))

	// Moved out of pending, into quarantined, retry metadata preserved.
	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.Empty(t, p)

	q, err := s.Quarantined()
	require.NoError(t, err)
	require.Len(t, q, 1)
	assert.Equal(t, "seg1", q[0].SegmentID)
	assert.Equal(t, 3, q[0].Attempts)
}

func TestSegmentEditOps_DeleteOp(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.RegisterOp("op2", removeOp("bar")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1"}))
	require.NoError(t, s.SnapshotSegments("op2", []string{"seg2"}))
	require.NoError(t, s.Quarantine("op1", "seg1")) // op1 has a quarantined row too... but it was the only pending
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1"}))

	require.NoError(t, s.DeleteOp("op1"))

	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "op2", ops[0].ID)

	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.Empty(t, p)
	q, err := s.Quarantined()
	require.NoError(t, err)
	for _, qs := range q {
		assert.NotEqual(t, "op1", qs.OpID)
	}

	// op2 untouched.
	p2, err := s.Pending("op2")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg2"}, p2)
}

func TestSegmentEditOps_ReconcileDeletesStaleSegmentRows(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2", "seg3"}))
	require.NoError(t, s.Quarantine("op1", "seg3"))

	// seg2 (pending) and seg3 (quarantined) no longer exist on disk.
	require.NoError(t, s.Reconcile(set("seg1"), nil))

	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg1"}, p)

	q, err := s.Quarantined()
	require.NoError(t, err)
	assert.Empty(t, q)

	// The operation itself survives (liveOpIDs nil = skip orphan sweep).
	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
}

func TestSegmentEditOps_ReconcileDeletesOrphanedOps(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("live", removeOp("foo")))
	require.NoError(t, s.RegisterOp("orphan", removeOp("bar")))
	require.NoError(t, s.SnapshotSegments("live", []string{"seg1"}))
	require.NoError(t, s.SnapshotSegments("orphan", []string{"seg1"}))

	require.NoError(t, s.Reconcile(set("seg1"), set("live")))

	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "live", ops[0].ID)

	// orphan's rows are gone; live's remain.
	all, err := s.AllPending()
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.Equal(t, "live", all[0].OpID)
}

func TestSegmentEditOps_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	s := newSegmentEditOps(dir, nil)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))
	require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("boom")))
	require.NoError(t, s.Close())

	reopened := newSegmentEditOps(dir, nil)
	defer reopened.Close()

	ops, err := reopened.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)

	all, err := reopened.AllPending()
	require.NoError(t, err)
	require.Len(t, all, 2)
	for _, p := range all {
		if p.SegmentID == "seg1" {
			assert.Equal(t, 1, p.Attempts)
			assert.Equal(t, "boom", p.LastError)
		}
	}
}

func TestSegmentEditOps_BumpAttemptDoesNotResurrectDoneSegment(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))
	require.NoError(t, s.MarkSegmentDone("op1", "seg1"))

	// A late/duplicate error for an already-completed segment must not revive it.
	require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("late error")))

	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg2"}, p)
}

func TestSegmentEditOps_SnapshotSegmentsRequiresRegisteredOp(t *testing.T) {
	s := newTestEditOps(t)

	err := s.SnapshotSegments("unregistered", []string{"seg1"})
	require.Error(t, err)
	require.ErrorContains(t, err, "not registered")

	// No orphan pending rows were created.
	p, err := s.Pending("unregistered")
	require.NoError(t, err)
	assert.Empty(t, p)
}

// TestSegmentEditOps_BuildCurrentTransformerDispatchesByOpType pins the design
// Dirk asked for: the persisted ops — via their OpType — drive which transformer
// runs. The bucket registers a factory per type; BuildCurrentTransformer selects
// only the registered types present in the sidecar and chains them in CreatedAt
// order. Op types with no factory are silently skipped.
func TestSegmentEditOps_BuildCurrentTransformerDispatchesByOpType(t *testing.T) {
	const (
		opA OpType = "op_a"
		opB OpType = "op_b"
	)
	// Each factory appends a tag so the output reveals which transformers ran and
	// in what order.
	tagFactory := func(tag string) OpTransformerFactory {
		return func(ops []ActiveOp) func([]byte) ([]byte, error) {
			return func(v []byte) ([]byte, error) { return append(v, tag...), nil }
		}
	}
	newStore := func(t *testing.T, transformers map[OpType]OpTransformerFactory) *SegmentEditOps {
		s := newSegmentEditOps(t.TempDir(), transformers)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		return s
	}

	t.Run("only registered op types contribute", func(t *testing.T) {
		s := newStore(t, map[OpType]OpTransformerFactory{opA: tagFactory("A")})
		require.NoError(t, s.RegisterOp("a1", OpDescriptor{Type: opA, CreatedAt: 1}))
		require.NoError(t, s.RegisterOp("b1", OpDescriptor{Type: opB, CreatedAt: 2})) // no factory

		transformer, applied, err := s.BuildCurrentTransformer()
		require.NoError(t, err)
		require.NotNil(t, transformer)
		out, err := transformer([]byte("v:"))
		require.NoError(t, err)
		assert.Equal(t, "v:A", string(out))
		require.Len(t, applied, 1)
		assert.Equal(t, "a1", applied[0].ID)
	})

	t.Run("multiple types chain in CreatedAt order", func(t *testing.T) {
		s := newStore(t, map[OpType]OpTransformerFactory{opA: tagFactory("A"), opB: tagFactory("B")})
		require.NoError(t, s.RegisterOp("b1", OpDescriptor{Type: opB, CreatedAt: 1}))
		require.NoError(t, s.RegisterOp("a1", OpDescriptor{Type: opA, CreatedAt: 2}))

		transformer, applied, err := s.BuildCurrentTransformer()
		require.NoError(t, err)
		out, err := transformer([]byte("v:"))
		require.NoError(t, err)
		assert.Equal(t, "v:BA", string(out), "opB (CreatedAt 1) must apply before opA (CreatedAt 2)")
		require.Len(t, applied, 2)
	})

	t.Run("no registered type present yields nil transformer", func(t *testing.T) {
		s := newStore(t, map[OpType]OpTransformerFactory{opA: tagFactory("A")})
		require.NoError(t, s.RegisterOp("b1", OpDescriptor{Type: opB, CreatedAt: 1}))

		transformer, applied, err := s.BuildCurrentTransformer()
		require.NoError(t, err)
		assert.Nil(t, transformer)
		assert.Nil(t, applied)
	})

	t.Run("empty registry yields nil transformer", func(t *testing.T) {
		s := newStore(t, nil)
		require.NoError(t, s.RegisterOp("a1", OpDescriptor{Type: opA, CreatedAt: 1}))

		transformer, applied, err := s.BuildCurrentTransformer()
		require.NoError(t, err)
		assert.Nil(t, transformer)
		assert.Nil(t, applied)
	})
}

// TestSegmentEditOps_WarnsOnMissingTransformer pins the observability guard: when
// the sidecar holds an op whose type has no registered transformer (a forgotten
// WithEditOpTransformers entry, or a leftover after downgrade), BuildCurrentTransformer
// skips it safely but logs a warning — once per type, not once per pass — so the
// stalled operation is visible instead of silently never completing.
func TestSegmentEditOps_WarnsOnMissingTransformer(t *testing.T) {
	const (
		opA OpType = "op_a"
		opB OpType = "op_b" // no factory registered for this type
	)
	logger, hook := test.NewNullLogger()
	s := newSegmentEditOps(t.TempDir(), map[OpType]OpTransformerFactory{
		opA: func(ops []ActiveOp) func([]byte) ([]byte, error) {
			return func(v []byte) ([]byte, error) { return v, nil }
		},
	})
	s.logger = logger
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.RegisterOp("a1", OpDescriptor{Type: opA, CreatedAt: 1}))
	require.NoError(t, s.RegisterOp("b1", OpDescriptor{Type: opB, CreatedAt: 2}))

	_, applied, err := s.BuildCurrentTransformer()
	require.NoError(t, err)
	require.Len(t, applied, 1, "only the op with a registered transformer is applied")

	warns := warnEntries(hook)
	require.Len(t, warns, 1, "exactly one warning for the unsupported op type")
	assert.Equal(t, opB, warns[0].Data["op_type"])
	assert.Equal(t, "b1", warns[0].Data["op_id"])

	// Deduped: re-running the pass over the same unsupported type does not re-log.
	_, _, err = s.BuildCurrentTransformer()
	require.NoError(t, err)
	require.Len(t, warnEntries(hook), 1, "the warning is emitted once per op type, not once per pass")
}

func warnEntries(hook *test.Hook) []*logrus.Entry {
	var out []*logrus.Entry
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel {
			out = append(out, e)
		}
	}
	return out
}

// TestSegmentEditOps_ReadPathsNeverCreateSidecar pins the headline lazy-open
// invariant: every read/bookkeeping path is a clean no-op on a store that has
// never seen an op, and crucially none of them materializes the bolt file. The
// constantly-running compaction/cleanup/reconcile cycles call these on every
// idle objects bucket; a single ensureOpen slip here would litter the data
// directory with empty sidecars (breaking backups and disk accounting). Each
// case runs against a fresh store so a leak in one method can't be masked by
// another.
func TestSegmentEditOps_ReadPathsNeverCreateSidecar(t *testing.T) {
	tests := []struct {
		name string
		call func(t *testing.T, s *SegmentEditOps)
	}{
		{"LoadOps", func(t *testing.T, s *SegmentEditOps) {
			ops, err := s.LoadOps()
			require.NoError(t, err)
			assert.Empty(t, ops)
		}},
		{"Pending", func(t *testing.T, s *SegmentEditOps) {
			p, err := s.Pending("op1")
			require.NoError(t, err)
			assert.Empty(t, p)
		}},
		{"AllPending", func(t *testing.T, s *SegmentEditOps) {
			p, err := s.AllPending()
			require.NoError(t, err)
			assert.Empty(t, p)
		}},
		{"Quarantined", func(t *testing.T, s *SegmentEditOps) {
			q, err := s.Quarantined()
			require.NoError(t, err)
			assert.Empty(t, q)
		}},
		{"MarkSegmentDone", func(t *testing.T, s *SegmentEditOps) {
			require.NoError(t, s.MarkSegmentDone("op1", "seg1"))
		}},
		{"BumpAttempt", func(t *testing.T, s *SegmentEditOps) {
			require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("boom")))
		}},
		{"Quarantine", func(t *testing.T, s *SegmentEditOps) {
			require.NoError(t, s.Quarantine("op1", "seg1"))
		}},
		{"DeleteOp", func(t *testing.T, s *SegmentEditOps) {
			require.NoError(t, s.DeleteOp("op1"))
		}},
		{"Reconcile", func(t *testing.T, s *SegmentEditOps) {
			require.NoError(t, s.Reconcile(set("seg1"), set("op1")))
		}},
		{"RecordCompaction", func(t *testing.T, s *SegmentEditOps) {
			require.NoError(t, s.RecordCompaction("a", "b", nil))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s := newSegmentEditOps(dir, nil)
			t.Cleanup(func() { require.NoError(t, s.Close()) })

			tt.call(t, s)

			require.NoFileExists(t, filepath.Join(dir, segmentEditOpsFileName),
				"%s must not materialize the sidecar on an idle store", tt.name)
		})
	}
}

// TestSegmentEditOps_ConcurrentOpenIsSafe exercises the mu-guarded one-time open
// under -race: many goroutines racing to register the first op must converge on
// a single bolt handle with no torn open and no lost write.
func TestSegmentEditOps_ConcurrentOpenIsSafe(t *testing.T) {
	s := newTestEditOps(t)

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			errs[i] = s.RegisterOp(fmt.Sprintf("op%d", i), removeOp("foo"))
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, n)
}

// TestSegmentEditOps_CloseWithoutOpenIsNoop pins that Close on a never-opened
// store (the common idle-bucket shutdown) is a clean no-op and creates nothing.
func TestSegmentEditOps_CloseWithoutOpenIsNoop(t *testing.T) {
	dir := t.TempDir()
	s := newSegmentEditOps(dir, nil)
	require.NoError(t, s.Close())
	require.NoFileExists(t, filepath.Join(dir, segmentEditOpsFileName))
}
