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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
)

type taskListerStub struct{ tasks []*distributedtask.Task }

func (s taskListerStub) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return map[string][]*distributedtask.Task{ReindexNamespace: s.tasks}, nil
}

// TestShouldDeferBlockmaxFlip_StampBreaksWedge pins the same-tick sibling
// wedge fix: reading the durable stamp (not the FINISHED-task list) lets the
// second of two siblings completing in the same tick flip the class flag,
// instead of both wedging forever.
func TestShouldDeferBlockmaxFlip_StampBreaksWedge(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	tr := true

	newProvider := func(cls *models.Class, tasks []*distributedtask.Task) *ReindexProvider {
		return &ReindexProvider{
			logger:        logger,
			schemaManager: &schema.Manager{SchemaReader: classReaderStub{class: cls}},
			taskLister:    taskListerStub{tasks: tasks},
		}
	}

	classWith := func(siblingStamp *bool) *models.Class {
		return &models.Class{
			Class:               "Docs",
			InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: false}, // partial: never flipped
			Properties: []*models.Property{
				{Name: "propX", DataType: []string{"text"}, IndexSearchable: &tr, SearchableBlockmax: siblingStamp}, // sibling
				{Name: "propY", DataType: []string{"text"}, IndexSearchable: &tr},                                   // completing now
			},
		}
	}

	// propY is completing its change-algorithm cutover; the empty task list means
	// propX's FINISHED task has aged out (or is not yet FINISHED in the same tick).
	completeY := &ReindexTaskPayload{
		MigrationType: ReindexTypeChangeAlgorithm, Collection: "Docs", Properties: []string{"propY"},
	}

	t.Run("sibling stamped blockmax → no spurious defer (wedge broken)", func(t *testing.T) {
		p := newProvider(classWith(&tr), nil)
		deferFlip, err := p.shouldDeferBlockmaxFlip(ctx, completeY, logger)
		require.NoError(t, err)
		require.False(t, deferFlip, "a stamped sibling must not force a defer once its task has aged out")
	})

	t.Run("sibling unstamped + aged-out task → defer (genuinely still WAND)", func(t *testing.T) {
		p := newProvider(classWith(nil), nil)
		deferFlip, err := p.shouldDeferBlockmaxFlip(ctx, completeY, logger)
		require.NoError(t, err)
		require.True(t, deferFlip, "an unstamped sibling with no live task is genuinely WAND → defer")
	})
}
