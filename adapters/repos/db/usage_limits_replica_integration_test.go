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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

const objectCap = 10

type fakeObjectCounter struct{ count int64 }

func (c *fakeObjectCounter) LocalObjectCount(context.Context, string) (int64, error) {
	return c.count, nil
}

// setupObjectCapShard is setupReplayShard with an object cap its counter
// reports reached, so every capped write is refused.
func setupObjectCapShard(t *testing.T) (repo *DB, idx *Index, shard string, class *models.Class) {
	t.Helper()
	repo, idx, shard, class = setupReplayShard(t)
	idx.usageLimits = usagelimits.NewManager(usagelimits.Config{
		MaxObjectsCount: runtime.NewDynamicValue(objectCap),
	}, &fakeObjectCounter{count: objectCap})
	return repo, idx, shard, class
}

func TestObjectCap_RepairAndReplayWritesSkipCap(t *testing.T) {
	tests := []struct {
		name  string
		write func(t *testing.T, idx *Index, shard string, obj *models.Object)
	}{
		{
			name: "OverwriteObjects",
			write: func(t *testing.T, idx *Index, shard string, obj *models.Object) {
				resp, err := idx.OverwriteObjects(context.Background(), shard, []*objects.VObject{{
					LatestObject:            obj,
					Vector:                  obj.Vector,
					LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnix,
				}})
				require.NoError(t, err)
				require.Empty(t, resp)
			},
		},
		{
			name: "OverwriteObjectsFromChangeLog",
			write: func(t *testing.T, idx *Index, shard string, obj *models.Object) {
				require.NoError(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, []ChangeLogReplayEntry{{
					ID:                      obj.ID,
					LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnix,
					Payload:                 mustMarshalPayload(t, obj, obj.Vector),
				}}))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo, idx, shard, class := setupObjectCapShard(t)
			now := time.Now().UnixMilli()
			obj := &models.Object{
				ID:                 "981c09f9-67f3-4e6e-a988-c53eaefbd58e",
				Class:              class.Class,
				CreationTimeUnix:   now,
				LastUpdateTimeUnix: now,
				Properties:         map[string]interface{}{"stringProp": "copied"},
				Vector:             []float32{1, 2, 3},
			}

			s, release, err := idx.GetShard(ctx, shard)
			require.NoError(t, err)
			defer release()
			errs := s.PutObjectBatch(ctx, []*storobj.Object{storobj.FromObject(obj, obj.Vector, nil, nil)})
			require.Len(t, errs, 1)
			var limitErr *usagelimits.LimitExceededError
			require.ErrorAs(t, errs[0], &limitErr)

			tt.write(t, idx, shard, obj)

			found, err := repo.Object(ctx, class.Class, obj.ID, nil, additional.Properties{}, nil, "")
			require.NoError(t, err)
			require.NotNil(t, found, "the copy must be written past the cap")
		})
	}
}
