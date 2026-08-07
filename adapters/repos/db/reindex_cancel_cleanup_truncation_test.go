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
	"io"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// failToLoadMonitor makes every shard load fail, and cancels the sweep's
// context on the first attempt so the walk aborts on the shard after it.
type failToLoadMonitor struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	calls  int
}

func (m *failToLoadMonitor) CheckAlloc(sizeInBytes int64) error { return nil }

func (m *failToLoadMonitor) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == 1 && m.cancel != nil {
		m.cancel()
	}
	return errors.New("memory pressure")
}

func (m *failToLoadMonitor) Refresh(updateMappings bool) {}

// The sweep runs with the collection's backup and restore gate closed, and the
// caller logs what it got. A sweep that ran out of context part-way through
// left the shards after that point untouched — if the only thing it reports is
// the shard that failed before it, the caller reads a bounded failure where the
// truth is "unknown, from here on".
func TestCleanStalePartialReindexStateReportsATruncatedSweep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	monitor := &failToLoadMonitor{cancel: cancel}

	logger := logrus.New()
	logger.SetOutput(io.Discard)
	idx := &Index{
		Config:     IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
		closingCtx: context.Background(),
		logger:     logger,
	}
	for _, name := range []string{"shard-a", "shard-b"} {
		(*sync.Map)(&idx.shards).Store(name, &LazyLoadShard{
			shardOpts:  &deferredShardOpts{name: name, index: idx, class: &models.Class{Class: "Movies"}},
			memMonitor: monitor,
		})
	}

	// An index type the bucket-name mapping does not know reads as "cannot tell
	// whether there is state here", which is what puts every shard on the
	// sweep's list without any on-disk fixture.
	err := idx.CleanStalePartialReindexState(ctx, "title", "an-index-type-this-build-does-not-know")

	require.Error(t, err)
	require.ErrorIs(t, err, ErrCleanupSweepTruncated,
		"the walk stopped early, so the shards after it were never swept and the caller has to know")
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "unwrap for partial-reindex cleanup",
		"the shard that failed before the abort must still be reported")
}
