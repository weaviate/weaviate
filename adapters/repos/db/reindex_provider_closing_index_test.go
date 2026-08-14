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

// A closing index must not be reported as a node whose swap finished.
//
// The scheduler treats "callbacks done" as terminal: it stops re-firing
// OnGroupCompleted, so a node that answers true while it still holds an
// untidied tracker keeps the old tokenization after the cluster-wide schema
// flip already committed. The lenient shard walk answers nil once the index
// is closing, so it visits nothing and every shard reads as one this node
// does not hold — which the tracker loop below skips, landing on true.
//
// The rows that signal a close therefore fail if the walk is swapped back to
// [Index.ForEachShard]: the two tidied ones have nothing untidied on disk, so
// the only thing that can produce false is the walk refusing to answer.
func TestLocalCallbacksDoneRefusesToAnswerForAClosingIndex(t *testing.T) {
	const (
		prop   = "title"
		tenant = "cold-tenant"
		node   = "n1"
	)

	for _, tc := range []struct {
		name string
		// closeRequested signals a delete that has not reached teardown yet;
		// closeCause only answers once teardown cancels closingCtx.
		closeRequested bool
		closing        bool
		// sentinels are written into the tenant's tracker dir.
		sentinels []string
		want      bool
	}{
		// The untidied, non-closing baseline is already pinned in
		// TestLocalCallbacksDoneLeavesUnloadedShardsAlone; an untidied tracker
		// answers false with or without closing (row below), so it wouldn't
		// isolate what closing changes here.
		{
			name:      "an open index whose swap tidied",
			sentinels: []string{"started.mig", "tidied.mig"},
			want:      true,
		},
		{
			name:      "a closing index still holding an untidied tracker",
			closing:   true,
			sentinels: []string{"started.mig"},
			want:      false,
		},
		{
			name:      "a closing index whose tracker tidied is still unanswerable",
			closing:   true,
			sentinels: []string{"started.mig", "tidied.mig"},
			want:      false,
		},
		{
			name:           "a delete committed before teardown reached the index",
			closeRequested: true,
			sentinels:      []string{"started.mig", "tidied.mig"},
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
			mkTrackerDir(t, shardPathLSM(idx.path(), tenant),
				postMergeTrackerDir(t, prop), tc.sentinels...)

			// Drop order: the delete is committed first, teardown cancels
			// closingCtx after it has queued behind the index locks.
			if tc.closeRequested || tc.closing {
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
				nil, logger, node, nil, ctx)

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
