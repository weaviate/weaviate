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

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func closableIndex(class string) *Index {
	idx := &Index{Config: IndexConfig{ClassName: entschema.ClassName(class)}}
	idx.closeRequestedCtx, idx.signalCloseRequested = context.WithCancelCause(context.Background())
	return idx
}

func TestDrainIndex(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "T1", Version: 1}
	for _, tc := range []struct {
		name   string
		liveOn func(dropped, other *Index) *Index
	}{
		{name: "no unit is live"},
		{
			name:   "a unit on another collection does not hold the drop",
			liveOn: func(_, other *Index) *Index { return other },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			p := structuralInvariantNewBareProvider()
			p.logger, p.serverCtx = logger, context.Background()
			dropped, other := closableIndex("C"), closableIndex("D")
			running := map[string]context.Context{}
			for i, collection := range []string{"C", "D"} {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				d := distributedtask.TaskDescriptor{ID: collection, Version: uint64(i + 1)}
				p.runningHandles[d] = &reindexTaskHandle{cancel: cancel, doneCh: make(chan struct{})}
				p.payloads[d] = &ReindexTaskPayload{Collection: collection}
				running[collection] = ctx
			}
			if tc.liveOn != nil {
				release, entered := p.enterLocalUnit(desc, "u1", tc.liveOn(dropped, other))
				require.True(t, entered)
				t.Cleanup(release)
			}

			dropped.signalCloseRequested(errIndexDropped)
			p.DrainIndex(dropped)

			require.Error(t, running["C"].Err(), "the dropped collection's work is cancelled")
			require.NoError(t, running["D"].Err())
			_, entered := p.enterLocalUnit(desc, "u2", dropped)
			require.False(t, entered, "no unit may start on an index being dropped")
			release, entered := p.enterLocalUnit(desc, "u2", other)
			require.True(t, entered)
			release()
		})
	}
}

func TestDeleteIndexWaitsForReindexUnits(t *testing.T) {
	ctx := testCtx()
	className := "DropDrain" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	_, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false,
		func(i *Index) {
			i.closeRequestedCtx, i.signalCloseRequested = context.WithCancelCause(context.Background())
		})
	registerIndex(idx, className)
	logger, _ := logrustest.NewNullLogger()
	p := structuralInvariantNewBareProvider()
	p.logger, p.serverCtx = logger, context.Background()
	idx.db.SetReindexIndexDrain(p.DrainIndex)

	release, entered := p.enterLocalUnit(distributedtask.TaskDescriptor{ID: "T1", Version: 1}, "u1", idx)
	require.True(t, entered)
	deleted := make(chan error, 1)
	go func() { deleted <- idx.db.DeleteIndex(entschema.ClassName(className)) }()

	select {
	case err := <-deleted:
		t.Fatalf("the drop ran while a reindex unit still held the index: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.DirExists(t, idx.path(), "the files stay until the unit releases")

	release()
	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the drop did not finish once the unit released")
	}
	require.NoDirExists(t, idx.path())
}
