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
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// TestLocalCallbacksDoneRefusesToAnswerForAClosingIndex pins that a closing
// index must not report a swap finished: the scheduler treats "callbacks
// done" as terminal and stops re-firing OnGroupCompleted, so answering true
// on an uncommitted migration would strand it on the old tokenization. The
// lenient shard walk answers nil while closing, which the record loop below
// reads as true; swapping back to [Index.ForEachShard] would fail these rows,
// since their migration is committed and only a refusing walk can produce
// false. The two close signals are raised independently so each row names
// the one it tests; teardown raises both.
func TestLocalCallbacksDoneRefusesToAnswerForAClosingIndex(t *testing.T) {
	const (
		prop   = "title"
		tenant = "cold-tenant"
		node   = "n1"
	)

	for _, tc := range []struct {
		name string
		// closeRequested is a delete committed against the collection; closing
		// is teardown having cancelled the index's context.
		closeRequested bool
		closing        bool
		// committed says whether the tenant's migration reached the state
		// from which its staged data is the data.
		committed bool
		want      bool
	}{
		// The uncommitted, non-closing baseline is pinned in
		// TestLocalCallbacksDoneLeavesUnloadedShardsAlone; it wouldn't isolate
		// what closing changes here.
		{
			name:      "an open index whose migration committed",
			committed: true,
			want:      true,
		},
		{
			name:    "a closing index still holding an uncommitted migration",
			closing: true,
			want:    false,
		},
		{
			name:      "teardown cancelled the index while its migration was committed",
			closing:   true,
			committed: true,
			want:      false,
		},
		{
			name:           "a delete committed before teardown reached the index",
			closeRequested: true,
			committed:      true,
			want:           false,
		},
		{
			name:           "both signals, which is what a drop actually raises",
			closeRequested: true,
			closing:        true,
			committed:      true,
			want:           false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "ClosingCallbacksDone_" + uuid.NewString()[:8]

			logger, _ := logrustest.NewNullLogger()
			closingCtx, closeIndex := context.WithCancel(context.Background())
			defer closeIndex()
			closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
			defer signalCloseRequested(nil)

			idx := &Index{
				Config:               IndexConfig{RootPath: t.TempDir(), ClassName: entschema.ClassName(className)},
				closingCtx:           closingCtx,
				closeRequestedCtx:    closeRequestedCtx,
				signalCloseRequested: signalCloseRequested,
				logger:               logger,
			}
			idx.shards.Store(tenant, &LazyLoadShard{
				shardOpts: &deferredShardOpts{name: tenant, index: idx},
			})
			state := MigrationStateIterating
			if tc.committed {
				state = MigrationStateMerged
			}
			mkMigrationRecordFor(t, shardPathLSM(idx.path(), tenant), postMergeTrackerDir(t, prop),
				"T_bootstrap", 1, "u1", ReindexTypeChangeTokenization, state, prop)

			if tc.closeRequested {
				signalCloseRequested(errIndexDropped)
			}
			if tc.closing {
				closeIndex()
			}

			payload, err := json.Marshal(ReindexTaskPayload{
				Collection:    className,
				MigrationType: ReindexTypeChangeTokenization,
				Properties:    []string{prop},
				UnitToShard:   map[string]string{"u1": tenant},
				UnitToNode:    map[string]string{"u1": node},
			})
			require.NoError(t, err)

			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
				nil, nil, logger, node, nil, ctx)

			got := p.LocalCallbacksDone(&distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_bootstrap", Version: 1},
				Status:         distributedtask.TaskStatusSwapping,
				Payload:        payload,
			}, node)

			require.Equal(t, tc.want, got)
		})
	}
}
