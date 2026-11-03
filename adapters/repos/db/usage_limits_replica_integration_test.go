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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

const (
	objectCap    = 10
	capNamespace = "alpha"
)

type fakeObjectCounter struct {
	count        int64
	err          error
	gotNamespace string
}

func (c *fakeObjectCounter) LocalObjectCount(_ context.Context, namespace string) (int64, error) {
	c.gotNamespace = namespace
	return c.count, c.err
}

// setupObjectCapShard is setupReplayShard for a class in capNamespace, with a
// cap of objectCap checked against what counter reports.
func setupObjectCapShard(t *testing.T, counter *fakeObjectCounter) (repo *DB, idx *Index, shard string, class *models.Class) {
	t.Helper()
	repo, idx, shard, class = setupReplayShardForClass(t, capNamespace+":ReplayClass",
		existerWithState(t, api.NamespaceStateActive))
	idx.usageLimits = usagelimits.NewManager(usagelimits.Config{
		MaxObjectsCount: runtime.NewDynamicValue(objectCap),
	}, counter)
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
			counter := &fakeObjectCounter{count: objectCap}
			repo, idx, shard, class := setupObjectCapShard(t, counter)
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
			require.Equal(t, capNamespace, counter.gotNamespace)

			tt.write(t, idx, shard, obj)

			found, err := repo.Object(ctx, class.Class, obj.ID, nil, additional.Properties{}, nil, "")
			require.NoError(t, err)
			require.NotNil(t, found, "the copy must be written past the cap")
		})
	}
}

func TestObjectCap_ReplicaPrepare(t *testing.T) {
	errCount := errors.New("count failed")
	tests := []struct {
		name    string
		counter fakeObjectCounter
		// batch is the object count of a preparePutObjects call, and 0 calls
		// preparePutObject with one object.
		batch    int
		wantCode replicaerrors.StatusCode
	}{
		{name: "single at the cap", counter: fakeObjectCounter{count: objectCap}, wantCode: replicaerrors.StatusUsageLimitExceeded},
		{name: "single filling the cap", counter: fakeObjectCounter{count: objectCap - 1}, wantCode: replicaerrors.StatusOK},
		{name: "batch crossing the cap", counter: fakeObjectCounter{count: objectCap - 2}, batch: 3, wantCode: replicaerrors.StatusUsageLimitExceeded},
		{name: "batch filling the cap", counter: fakeObjectCounter{count: objectCap - 3}, batch: 3, wantCode: replicaerrors.StatusOK},
		{name: "single with a failing counter", counter: fakeObjectCounter{err: errCount}, wantCode: replicaerrors.StatusConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo, idx, shard, class := setupObjectCapShard(t, &tt.counter)
			objs := make([]*storobj.Object, max(tt.batch, 1))
			for i := range objs {
				objs[i] = storobj.FromObject(&models.Object{
					ID:         strfmt.UUID(uuid.NewString()),
					Class:      class.Class,
					Properties: map[string]interface{}{"stringProp": "replicated"},
				}, []float32{1, 2, 3}, nil, nil)
			}

			const requestID = "prepare-under-cap"
			var resp replica.SimpleResponse
			if tt.batch == 0 {
				resp = idx.ReplicateObject(ctx, shard, requestID, objs[0], 0)
			} else {
				resp = idx.ReplicateObjects(ctx, shard, requestID, objs, 0)
			}
			idx.CommitReplication(ctx, shard, requestID)
			require.Equal(t, capNamespace, tt.counter.gotNamespace)

			prepared := tt.wantCode == replicaerrors.StatusOK
			if prepared {
				require.NoError(t, resp.FirstError())
			} else {
				var rejection *replicaerrors.Error
				require.ErrorAs(t, resp.FirstError(), &rejection)
				require.Equal(t, tt.wantCode, rejection.Code)
				require.NoError(t, rejection.Err, "the HTTP and gRPC replica clients carry no Err")
				if tt.wantCode == replicaerrors.StatusUsageLimitExceeded {
					limit, ok := usagelimits.FromBodyJSON([]byte(rejection.Msg))
					require.True(t, ok, "Msg is no limit body: %q", rejection.Msg)
					require.Equal(t, usagelimits.LimitObjects, limit.Limit)
					require.EqualValues(t, objectCap, limit.Value)
				}
			}
			for _, obj := range objs {
				found, err := repo.Object(ctx, class.Class, obj.ID(), nil, additional.Properties{}, nil, "")
				require.NoError(t, err)
				require.Equal(t, prepared, found != nil, "object %s written: %v", obj.ID(), found != nil)
			}
		})
	}
}
