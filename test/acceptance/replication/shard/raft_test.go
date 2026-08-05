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

package shard

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

const (
	UUID1 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241")
	UUID2 = strfmt.UUID("d1c9e5b8-9a3e-4c8b-9f1e-2a5f6b7c8d9e")
	UUID3 = strfmt.UUID("e2f3c4d5-6a7b-8c9d-0e1f-2a3b4c5d6e7f")
	UUID4 = strfmt.UUID("f1a2b3c4-5d6e-7f8a-9b0c-1d2e3f4a5b6c")
)

func Test_RaftShardReplication(t *testing.T) {
	// ctx := context.Background()

	// compose, err := docker.New().
	// 	WithWeaviateCluster(3).
	// 	Start(ctx)
	// require.NoError(t, err)
	// defer func() {
	// 	require.NoError(t, compose.Terminate(ctx))
	// }()

	// // Point client at node-0
	// helper.SetupClient(compose.GetWeaviate().URI())
	// hosts := []string{compose.GetWeaviate().URI(), compose.GetWeaviate().URI(), compose.GetWeaviate().URI()}
	helper.SetupClient("localhost:8080")
	hosts := []string{"localhost:8080", "localhost:8081", "localhost:8082"}

	cls := articles.ParagraphsClass()
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor:      3,
		RaftEnabled: true,
	}

	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	t.Run("create and update object", func(t *testing.T) {
		helper.CreateObject(t, articles.NewParagraph().WithContents("RAFT New").WithID(UUID1).Object())
		helper.UpdateObject(t, articles.NewParagraph().WithContents("RAFT New Updated").WithID(UUID1).Object())

		// Verify the new object and update eventually exist on all nodes
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, host := range hosts {
				obj, err := getObj(t, host, cls.Class, UUID1)
				assert.Nil(ct, err, "Object should exist on node at host %s", host)
				if !assert.NotNil(ct, obj, "Object should exist on node at host %s", host) {
					continue
				}
				assert.Equal(ct, "RAFT New Updated", obj.Properties.(map[string]any)["contents"], "Object should be updated on node at host %s", host)
			}
		}, 10*time.Second, 1*time.Second)
	})

	t.Run("create and delete object", func(t *testing.T) {
		helper.CreateObject(t, articles.NewParagraph().WithContents("RAFT To Delete").WithID(UUID2).Object())
		helper.DeleteObject(t, &models.Object{Class: cls.Class, ID: UUID2})
		// Verify the object is eventually deleted on all nodes
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, host := range hosts {
				_, err := getObj(t, host, cls.Class, UUID2)
				assert.NotNil(ct, err, "Object should be deleted on node at host %s", host)
			}
		}, 10*time.Second, 1*time.Second)
	})

	t.Run("create and eventually get object with EVENTUAL consistency", func(t *testing.T) {
		helper.CreateObject(t, articles.NewParagraph().WithContents("RAFT Eventual").WithID(UUID2).Object())
		// Verify the object is returned without considering EC by querying with EVENTUAL consistency
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, host := range hosts {
				obj, err := helper.GetObjectCL(t, cls.Class, UUID2, types.ConsistencyLevelEventual)
				assert.Nil(ct, err, "Object should exist on node at host %s", host)
				assert.NotNil(ct, obj, "Object should exist on node at host %s", host)
			}
		}, 10*time.Second, 1*time.Second)
	})

	t.Run("create and get object with STRONG consistency", func(t *testing.T) {
		helper.CreateObject(t, articles.NewParagraph().WithContents("RAFT Strong").WithID(UUID3).Object())
		// Verify the object is returned without considering EC by querying with STRONG consistency
		obj, err := helper.GetObjectCL(t, cls.Class, UUID3, types.ConsistencyLevelStrong)
		require.NoError(t, err)
		require.NotNil(t, obj)
	})

	t.Run("create and get object with DIRECT consistency", func(t *testing.T) {
		helper.CreateObject(t, articles.NewParagraph().WithContents("RAFT Direct").WithID(UUID4).Object())
		// Verify the object is returned without considering EC by querying with DIRECT consistency
		obj, err := helper.GetObjectCL(t, cls.Class, UUID4, types.ConsistencyLevelDirect)
		require.NoError(t, err)
		require.NotNil(t, obj)
	})
}

func getObj(t *testing.T, host, cls string, uuid strfmt.UUID) (*models.Object, error) {
	helper.SetupClient(host)
	return helper.GetObject(t, cls, uuid)
}
