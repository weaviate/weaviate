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

//go:build integrationTest

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

// midReindexTask runs mid once the reindex holds the store and created its temp
// buckets.
type midReindexTask struct {
	*ShardInvertedReindexTask_SpecifiedIndex
	mid func(shard ShardLike) error
}

func (t midReindexTask) ObjectsIterator(shard ShardLike) objectsIterator {
	iterate := t.ShardInvertedReindexTask_SpecifiedIndex.ObjectsIterator(shard)
	return func(ctx context.Context, fn func(*storobj.Object) error) error {
		if err := t.mid(shard); err != nil {
			return err
		}
		return iterate(ctx, fn)
	}
}

func TestReindexGivesTheStoreBack(t *testing.T) {
	cases := []struct {
		name    string
		mid     func(overlay *clusterschema.NodeOverlay, shard ShardLike) error
		wantErr bool
	}{
		{
			name: "a status sync leaves the temp buckets to the reindex",
			mid: func(_ *clusterschema.NodeOverlay, shard ShardLike) error {
				return shard.syncStorePause()
			},
		},
		{
			name: "resource pressure stops the reindex",
			mid: func(overlay *clusterschema.NodeOverlay, shard ShardLike) error {
				overlay.SetResourcePressure(true)
				return shard.syncStorePause()
			},
			wantErr: true,
		},
		{
			name: "a failed task",
			mid: func(*clusterschema.NodeOverlay, ShardLike) error {
				return errors.New("iterator failed")
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			class := createClassWithEverything(false, false)
			migrator, repo, schemaGetter := createRepo(t)
			defer repo.Shutdown(ctx)
			overlay := &clusterschema.NodeOverlay{}
			repo.SetNodeOverlay(overlay)
			require.NoError(t, migrator.AddClass(ctx, class))
			schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
			put := func() error {
				obj := &models.Object{
					ID: strfmt.UUID(uuid.NewString()), Class: class.Class,
					Properties: map[string]interface{}{"strings": []string{"a"}},
				}
				return repo.PutObject(ctx, obj, []float32{1}, nil, nil, nil, 0)
			}
			require.NoError(t, put())

			var shard ShardLike
			require.NoError(t, repo.GetIndex(schema.ClassName(class.Class)).ForEachShard(func(_ string, s ShardLike) error {
				shard = s
				return nil
			}))
			reindexer := NewShardInvertedReindexer(shard, repo.logger)
			reindexer.AddTask(midReindexTask{
				ShardInvertedReindexTask_SpecifiedIndex: &ShardInvertedReindexTask_SpecifiedIndex{
					classNamesWithPropertyNames: map[string]map[string]struct{}{class.Class: {"strings": {}}},
				},
				mid: func(shard ShardLike) error { return tc.mid(overlay, shard) },
			})

			err := reindexer.Do(ctx)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			overlay.SetResourcePressure(false)
			require.NoError(t, shard.syncStorePause())
			require.NoError(t, put(), "the shard takes writes once the reindex ends")
		})
	}
}
