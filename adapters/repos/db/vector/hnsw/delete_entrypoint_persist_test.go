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

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// entrypointPersistCall captures the state observed at the moment an
// entrypoint change was persisted to the commit log.
type entrypointPersistCall struct {
	id       uint64
	level    int
	memID    uint64
	memLevel int
	lockHeld bool
}

// entrypointPersistSpy wraps a CommitLogger and records, for every
// SetEntryPointWithMaxLayer call while armed, whether the caller held the
// hnsw main lock and whether the in-memory entrypoint state matched the
// persisted values at that moment. Both must hold: an entrypoint change
// persisted outside the critical section that performed the memory write can
// interleave with a concurrent entrypoint change, appending commit-log
// entries in the opposite order of the memory writes — after a restart the
// replayed entrypoint would diverge from what memory had.
type entrypointPersistSpy struct {
	CommitLogger
	h     *hnsw
	armed atomic.Bool

	mu    sync.Mutex
	calls []entrypointPersistCall
}

func (s *entrypointPersistSpy) SetEntryPointWithMaxLayer(id uint64, level int) error {
	if s.armed.Load() {
		// If the caller performs the persist inside its h.Lock() critical
		// section, TryLock must fail. The tests using this spy are
		// single-threaded, so a failing TryLock cannot be a false positive
		// from an unrelated concurrent lock holder.
		lockHeld := !s.h.TryLock()
		call := entrypointPersistCall{
			id:       id,
			level:    level,
			memID:    s.h.entryPointID,
			memLevel: s.h.currentMaximumLayer,
			lockHeld: lockHeld,
		}
		if !lockHeld {
			s.h.Unlock()
		}

		s.mu.Lock()
		s.calls = append(s.calls, call)
		s.mu.Unlock()
	}

	return s.CommitLogger.SetEntryPointWithMaxLayer(id, level)
}

func (s *entrypointPersistSpy) recorded() []entrypointPersistCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]entrypointPersistCall(nil), s.calls...)
}

// TestDeleteEntrypoint_PersistsUnderLock pins that every path replacing the
// global entrypoint after a delete persists the change to the commit log
// within the same h.Lock() critical section as the in-memory write, matching
// the pattern of repairGlobalEntrypoint. Delete() (holding deleteLock) and
// the tombstone-cleanup path replaceDeletedEntrypoint (holding resetLock) are
// not mutually exclusive, so a persist outside the lock lets the two
// interleave: memory updated in one order, commit log appended in the other.
func TestDeleteEntrypoint_PersistsUnderLock(t *testing.T) {
	tests := []struct {
		name    string
		replace func(t *testing.T, h *hnsw, entrypoint uint64)
	}{
		{
			// user-facing Delete of the current entrypoint
			name: "via Delete",
			replace: func(t *testing.T, h *hnsw, entrypoint uint64) {
				require.Nil(t, h.Delete(entrypoint))
			},
		},
		{
			// tombstone-cleanup cycle discovering a tombstoned entrypoint
			name: "via replaceDeletedEntrypoint",
			replace: func(t *testing.T, h *hnsw, entrypoint uint64) {
				require.Nil(t, h.addTombstone(entrypoint))
				ok, err := h.replaceDeletedEntrypoint(
					h.tombstonesAsDenyList(), func() bool { return false })
				require.Nil(t, err)
				require.True(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			vectors := vectorsForDeleteTest()

			store := testinghelpers.NewDummyStore(t)
			defer store.Shutdown(ctx)

			spy := &entrypointPersistSpy{CommitLogger: &NoopCommitLogger{}}
			index, err := New(Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "delete-entrypoint-persist-test",
				MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) { return spy, nil },
				DistanceProvider:      distancer.NewCosineDistanceProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
				GetViewThunk:                 GetViewThunk,
				TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
				AllocChecker:                 memwatch.NewDummyMonitor(),
			}, ent.UserConfig{
				MaxConnections:        30,
				EFConstruction:        128,
				VectorCacheMaxObjects: 100000,
			}, cyclemanager.NewCallbackGroupNoop(), store)
			require.Nil(t, err)
			spy.h = index

			for i, vec := range vectors {
				require.Nil(t, index.Add(ctx, uint64(i), vec))
			}

			oldEntrypoint := index.getEntrypoint()
			spy.armed.Store(true)
			tt.replace(t, index, oldEntrypoint)
			spy.armed.Store(false)

			calls := spy.recorded()
			require.NotEmpty(t, calls,
				"replacing the entrypoint must persist the change")
			for _, call := range calls {
				assert.NotEqual(t, oldEntrypoint, call.id,
					"new entrypoint must differ from the deleted one")
				assert.True(t, call.lockHeld,
					"entrypoint persist must happen under the hnsw main lock")
				assert.Equal(t, call.memID, call.id,
					"persisted entrypoint must match in-memory entrypoint")
				assert.Equal(t, call.memLevel, call.level,
					"persisted max layer must match in-memory max layer")
			}

			require.Nil(t, index.Drop(ctx, false))
		})
	}
}
