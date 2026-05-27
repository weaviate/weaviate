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

package changelog_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// TestSet_RaceWithWriter concurrently churns Register/Unregister of two
// op-ids against a writer that snapshots the set and appends to every log
// present. This is the phases.md "Set activate/deactivate races with writer"
// check; running under -race validates there are no data races in the
// copy-on-write path, and the post-run tally proves no writes were silently
// dropped.
//
// It also implicitly covers WithAdded, WithRemoved (including the empty-set
// nil return), ForEach, and Register/Unregister under CAS contention —
// deliberately in place of any isolated CRUD unit tests.
func TestSet_RaceWithWriter(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	logA := newLog(t)
	logB := newLog(t)

	var ptr atomic.Pointer[changelog.Set]

	var (
		done      atomic.Bool
		churnWG   sync.WaitGroup
		writerWG  sync.WaitGroup
		appendedA atomic.Int64
		appendedB atomic.Int64
	)

	churn := func(opID string, log *changelog.ChangeLog) {
		defer churnWG.Done()
		for !done.Load() {
			changelog.Register(&ptr, opID, log)
			changelog.Unregister(&ptr, opID)
		}
	}

	churnWG.Add(2)
	enterrors.GoWrapper(func() { churn("a", logA) }, logger)
	enterrors.GoWrapper(func() { churn("b", logB) }, logger)

	// Give the churn goroutines a moment to start spinning before we start
	// the writer, so the writer consistently observes non-nil snapshots.
	time.Sleep(5 * time.Millisecond)

	writerWG.Add(1)
	enterrors.GoWrapper(func() {
		defer writerWG.Done()
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			snap := ptr.Load()
			if snap == nil {
				continue
			}
			snap.ForEach(func(opID string, log *changelog.ChangeLog) {
				if _, err := log.AppendPut([16]byte{}, 0, []byte(opID)); err == nil {
					switch opID {
					case "a":
						appendedA.Add(1)
					case "b":
						appendedB.Add(1)
					}
				}
			})
		}
	}, logger)

	writerWG.Wait()
	done.Store(true)
	churnWG.Wait()

	// Finalize returns the highest assigned LSN, which equals the number of
	// successful appends. If any append was silently dropped by the
	// copy-on-write interaction, this comparison fails.
	finalA, err := logA.Finalize()
	require.NoError(t, err)
	require.Equal(t, appendedA.Load(), int64(finalA),
		"logA: writer counted %d appends but log reports %d", appendedA.Load(), finalA)

	finalB, err := logB.Finalize()
	require.NoError(t, err)
	require.Equal(t, appendedB.Load(), int64(finalB),
		"logB: writer counted %d appends but log reports %d", appendedB.Load(), finalB)

	// After both Unregister spins complete, the pointer must eventually
	// settle on nil (proving WithRemoved returns nil on the last entry and
	// Unregister stores that through the atomic). The churn loop above ends
	// with Unregister, so the last observable action on each op-id is a
	// removal.
	require.Nil(t, ptr.Load(),
		"after churn shutdown, set pointer should be nil; got %+v", ptr.Load())
}
