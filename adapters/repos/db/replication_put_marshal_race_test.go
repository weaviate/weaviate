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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	entities "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
)

// Pins the CL=ONE overlap where the local in-process commit leg assigns DocIDs while the broadcast leg still marshals the same objects.
func TestReplicatedPutCommitDoesNotRaceWireMarshal(t *testing.T) {
	className := "PutMarshalRace"
	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.UserConfig{Skip: true},
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{{
			Name:         "stringProp",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}},
	}
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	idx := db.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)
	var shardName string
	require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	}))
	rc := newRoutingReplicationClient(nil, db, stubResolver{localAddr: localHostAddr}, replLocalNode)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	newObj := func() *storobj.Object {
		return &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 strfmt.UUID(uuid.NewString()),
				Class:              className,
				Properties:         map[string]interface{}{"stringProp": "x"},
				LastUpdateTimeUnix: 1_000,
			},
		}
	}
	commit := func(t *testing.T, reqID string) {
		resp := replica.SimpleResponse{}
		require.NoError(t, rc.Commit(ctx, localHostAddr, className, shardName, reqID, &resp))
		require.NoError(t, resp.FirstError())
	}

	t.Run("single", func(t *testing.T) {
		for i := 0; i < 30; i++ {
			obj := newObj()
			reqID := fmt.Sprintf("put-marshal-race-%d", i)
			resp, err := rc.PutObject(ctx, localHostAddr, className, shardName, reqID, obj, 0)
			require.NoError(t, err)
			require.NoError(t, resp.FirstError())
			done := make(chan struct{})
			entities.GoWrapper(func() {
				defer close(done)
				_, _ = shared.IndicesPayloads.SingleObject.Marshal(obj, shared.MethodPut)
			}, logger)
			commit(t, reqID)
			<-done
			require.Zero(t, obj.DocID)
		}
	})

	t.Run("batch", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			objs := make([]*storobj.Object, 20)
			for j := range objs {
				objs[j] = newObj()
			}
			reqID := fmt.Sprintf("batch-marshal-race-%d", i)
			resp, err := rc.PutObjects(ctx, localHostAddr, className, shardName, reqID, objs, 0)
			require.NoError(t, err)
			require.NoError(t, resp.FirstError())
			done := make(chan struct{})
			entities.GoWrapper(func() {
				defer close(done)
				_, _ = shared.IndicesPayloads.ObjectList.Marshal(objs, shared.MethodPut)
			}, logger)
			commit(t, reqID)
			<-done
			for _, o := range objs {
				require.Zero(t, o.DocID)
			}
		}
	})
}
