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
	"github.com/weaviate/weaviate/entities/storobj"
)

// Pins the CL=ONE overlap where the local commit leg assigns DocIDs while the
// broadcast leg is still marshalling the same objects for a remote replica.
func TestReplicatedPutCommitDoesNotRaceWireMarshal(t *testing.T) {
	ctx := context.Background()
	shardLike, _ := testShard(t, ctx, "PutMarshalRace")
	shard, ok := shardLike.(*Shard)
	require.True(t, ok)
	logger, _ := test.NewNullLogger()
	newObj := func() *storobj.Object {
		return &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 strfmt.UUID(uuid.NewString()),
				Class:              "PutMarshalRace",
				Properties:         map[string]interface{}{"stringProp": "x"},
				LastUpdateTimeUnix: 1_000,
			},
		}
	}

	t.Run("single", func(t *testing.T) {
		for i := 0; i < 30; i++ {
			obj := newObj()
			reqID := fmt.Sprintf("put-marshal-race-%d", i)
			require.Empty(t, shard.preparePutObject(ctx, reqID, obj).Errors)
			done := make(chan struct{})
			entities.GoWrapper(func() {
				defer close(done)
				_, _ = shared.IndicesPayloads.SingleObject.Marshal(obj, shared.MethodPut)
			}, logger)
			shard.commitReplication(ctx, reqID)
			<-done
		}
	})

	t.Run("batch", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			objs := make([]*storobj.Object, 20)
			for j := range objs {
				objs[j] = newObj()
			}
			reqID := fmt.Sprintf("batch-marshal-race-%d", i)
			require.Empty(t, shard.preparePutObjects(ctx, reqID, objs).Errors)
			done := make(chan struct{})
			entities.GoWrapper(func() {
				defer close(done)
				_, _ = shared.IndicesPayloads.ObjectList.Marshal(objs, shared.MethodPut)
			}, logger)
			shard.commitReplication(ctx, reqID)
			<-done
		}
	})
}
