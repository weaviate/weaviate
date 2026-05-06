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

package hnsw

// Regression coverage for https://github.com/weaviate/0-weaviate-issues/issues/204:
//
// Prior to the fix, hnswCommitLogger.Drop and hnswCommitLogger.Shutdown
// blocked until their context expired whenever a compaction cycle was in
// flight. The compactor did not honor the cyclemanager's shouldAbort
// callback, so Unregister(ctx) waited for the cycle to complete naturally;
// when ctx expired first, Drop returned the deadline error before reaching
// its own RemoveAll, leaving .hnsw.commitlog.d/ orphaned on disk. The same
// uncancelable-cycle defect made shutdowns block for the full ctx budget
// shared with the rest of the node teardown.
//
// In production the trigger is the compactv2 startup migration: it places
// freshly-renamed snapshot files into the commitlog directory, the next
// maintenance tick starts compacting the entire dataset, and any concurrent
// shard drop or node shutdown within the ctx budget hits this race.
//
// The test below uses a custom FS that adds a small, deterministic delay
// to every fs.Open() of a .condensed file. With several condensed files
// pre-seeded into the directory, an unaborted RunCycle would take
// numFiles*perOpenDelay to complete. The cleanup operation under test is
// invoked with a ctx shorter than that, so a compactor that ignores
// shouldAbort runs past the deadline and the operation fails. After the
// fix, the compactor polls shouldAbort between files: the in-flight
// conversion always finishes (~ one perOpenDelay), then the loop sees the
// abort and yields, and Drop / Shutdown return well within ctx.
//
// Timing budget (all wall-clock; deterministic on any reasonable runner):
//   numFiles * perOpenDelay = 5 * 100ms  = 500ms total compaction
//   ctxBudget               = 200ms                   <  total → pre-fix fails
//   one in-flight file      = 100ms + ε  <  ctxBudget        → post-fix passes

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

const (
	cancelTestNumFiles     = 5
	cancelTestPerOpenDelay = 100 * time.Millisecond
	cancelTestCtxBudget    = 200 * time.Millisecond
)

// slowOpenFS wraps any common.FS and adds a deterministic per-Open delay
// for files matching suffix. opens is incremented before each delay so the
// test can detect "compaction has begun" without time-based polling.
type slowOpenFS struct {
	common.FS
	suffix       string
	perOpenDelay time.Duration
	opens        atomic.Int64
	startedOnce  sync.Once
	startedFired chan struct{}
}

func (f *slowOpenFS) Open(name string) (common.File, error) {
	if strings.HasSuffix(name, f.suffix) {
		f.opens.Add(1)
		f.startedOnce.Do(func() { close(f.startedFired) })
		time.Sleep(f.perOpenDelay)
	}
	return f.FS.Open(name)
}

func TestCommitLoggerCancelableCompaction(t *testing.T) {
	cases := []struct {
		name string
		// invoke is the production cleanup call under test. It receives a
		// short-deadline ctx; with the bug it blocks until the deadline
		// and returns the wrapped "deadline exceeded" error.
		invoke func(cl *hnswCommitLogger, ctx context.Context) error
		// assertCleanState verifies the post-condition that is specific
		// to this cleanup operation. For Drop the directory must be
		// removed (issue #204's user-visible symptom); for Shutdown the
		// directory must remain intact so the next startup can load it.
		assertCleanState func(t *testing.T, commitDir string)
	}{
		{
			name: "Drop",
			invoke: func(cl *hnswCommitLogger, ctx context.Context) error {
				return cl.Drop(ctx, false)
			},
			assertCleanState: func(t *testing.T, commitDir string) {
				_, statErr := os.Stat(commitDir)
				assert.True(t, os.IsNotExist(statErr),
					"commitlog dir %q must be removed by Drop, "+
						"not orphaned on disk (issue #204).", commitDir)
			},
		},
		{
			name: "Shutdown",
			invoke: func(cl *hnswCommitLogger, ctx context.Context) error {
				return cl.Shutdown(ctx)
			},
			assertCleanState: func(t *testing.T, commitDir string) {
				_, statErr := os.Stat(commitDir)
				assert.NoError(t, statErr,
					"commitlog dir %q must remain on disk after a "+
						"clean Shutdown (Shutdown is non-destructive).",
					commitDir)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rootDir := t.TempDir()
			indexName := "compactv2_cancel_repro"
			commitDir := commitLogDirectory(rootDir, indexName)
			require.NoError(t, os.MkdirAll(commitDir, 0o755))

			// Seed enough .condensed files that one full RunCycle takes
			// noticeably longer than ctxBudget. Empty content is fine:
			// inMemReader.Do gracefully handles an immediate io.EOF and
			// produces an empty .sorted output.
			for i := 0; i < cancelTestNumFiles; i++ {
				path := filepath.Join(commitDir,
					fmt.Sprintf("%d.condensed", 1000000000+i))
				require.NoError(t, os.WriteFile(path, nil, 0o644))
			}

			fs := &slowOpenFS{
				FS:           common.NewOSFS(),
				suffix:       ".condensed",
				perOpenDelay: cancelTestPerOpenDelay,
				startedFired: make(chan struct{}),
			}

			logger, _ := test.NewNullLogger()
			callbacks := cyclemanager.NewCallbackGroup(
				"compactv2_cancel_test_"+tc.name, logger, 1)

			cl, err := NewCommitLogger(rootDir, indexName, logger, callbacks, WithFS(fs))
			require.NoError(t, err)
			cl.InitMaintenance()

			// Drive the maintenance callback the same way the cycle manager
			// would, but synchronously from a goroutine we own. This sets
			// the callback's runningCtx, which is what makes Unregister
			// actually wait.
			cycleDone := make(chan struct{})
			go func() {
				defer close(cycleDone)
				callbacks.CycleCallback(func() bool { return false })
			}()

			// Always join the cycle goroutine before the test returns,
			// otherwise it leaks across t.Run sub-tests.
			t.Cleanup(func() {
				select {
				case <-cycleDone:
				case <-time.After(15 * time.Second):
					t.Error("cycle goroutine did not exit")
				}
			})

			// Wait for the compactor to actually enter the file-by-file
			// processing loop. Channel-based signal, no time-based polling.
			select {
			case <-fs.startedFired:
			case <-time.After(10 * time.Second):
				t.Fatal("compaction did not begin processing files")
			}

			opCtx, opCancel := context.WithTimeout(context.Background(), cancelTestCtxBudget)
			defer opCancel()

			opStart := time.Now()
			opErr := tc.invoke(cl, opCtx)
			opElapsed := time.Since(opStart)

			// Sanity rails. The op must take long enough that we know we
			// actually exercised the abort path (a couple file delays at
			// minimum, since the compaction was already running) and must
			// not run far past ctx (which would mean cyclemanager's
			// cooperative-abort plumbing is broken at a different layer
			// than the one we're testing).
			require.GreaterOrEqual(t, opElapsed, 50*time.Millisecond,
				"%s returned implausibly fast — was the cycle actually running?",
				tc.name)
			require.Less(t, opElapsed, 5*time.Second,
				"%s ran far past its ctx deadline; cyclemanager wait is broken",
				tc.name)

			// ============ Issue #204 invariants — these are the fix ============

			assert.NoError(t, opErr,
				"%s must succeed while a compaction is in flight: the compactor "+
					"must honor the cyclemanager's shouldAbort signal so Unregister "+
					"can return promptly (issue #204).", tc.name)

			tc.assertCleanState(t, commitDir)

			// As an additional invariant: a fix that polls shouldAbort
			// between files must not have processed every file before
			// returning. Otherwise the fix is "the cycle just happened to
			// finish before ctx expired" — coincidence, not cancellation.
			assert.Less(t, fs.opens.Load(), int64(cancelTestNumFiles),
				"%s returned but the cycle still processed all %d files; "+
					"abort plumbing is not actually short-circuiting the loop",
				tc.name, cancelTestNumFiles)
		})
	}
}
