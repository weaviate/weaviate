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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

const (
	UUID1 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241")
	UUID2 = strfmt.UUID("d1c9e5b8-9a3e-4c8b-9f1e-2a5f6b7c8d9e")
)

func Test_RaftShardReplication(t *testing.T) {
	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor:      3,
		RaftEnabled: true,
	}

	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	t.Run("add object", func(t *testing.T) {
		helper.CreateObject(t, articles.NewParagraph().WithContents("RAFT").WithID(UUID1).Object())
		// Verify the object eventually exists on all nodes
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, port := range []string{"8080", "8081", "8082"} {
				obj, err := getObj(t, port, cls.Class, UUID1)
				assert.Nil(ct, err, "Object should exist on node at port %s", port)
				assert.NotNil(ct, obj, "Object should exist on node at port %s", port)
			}
		}, 10*time.Second, 1*time.Second)
	})

	t.Run("update object", func(t *testing.T) {
		helper.UpdateObject(t, articles.NewParagraph().WithContents("RAFT Updated").WithID(UUID1).Object())
		// Verify the update eventually exists on all nodes
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, port := range []string{"8080", "8081", "8082"} {
				obj, err := getObj(t, port, cls.Class, UUID1)
				assert.Nil(ct, err, "Object should exist on node at port %s", port)
				assert.NotNil(ct, obj, "Object should exist on node at port %s", port)
				assert.Equal(ct, "RAFT Updated", obj.Properties.(map[string]any)["contents"], "Object should be updated on node at port %s", port)
			}
		}, 10*time.Second, 1*time.Second)
	})

	t.Run("create and update object", func(t *testing.T) {
		helper.CreateObject(t, articles.NewParagraph().WithContents("RAFT New").WithID(UUID2).Object())
		helper.UpdateObject(t, articles.NewParagraph().WithContents("RAFT New Updated").WithID(UUID2).Object())

		// Verify the new object and update eventually exist on all nodes
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, port := range []string{"8080", "8081", "8082"} {
				obj, err := getObj(t, port, cls.Class, UUID2)
				assert.Nil(ct, err, "Object should exist on node at port %s", port)
				assert.NotNil(ct, obj, "Object should exist on node at port %s", port)
				assert.Equal(ct, "RAFT New Updated", obj.Properties.(map[string]any)["contents"], "Object should be updated on node at port %s", port)
			}
		}, 10*time.Second, 1*time.Second)
	})

	t.Run("delete object", func(t *testing.T) {
		helper.DeleteObject(t, &models.Object{Class: cls.Class, ID: UUID2})
		// Verify the object is eventually deleted on all nodes
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, port := range []string{"8080", "8081", "8082"} {
				_, err := getObj(t, port, cls.Class, UUID2)
				assert.NotNil(ct, err, "Object should be deleted on node at port %s", port)
			}
		}, 10*time.Second, 1*time.Second)
	})
}

func getObj(t *testing.T, port, cls string, uuid strfmt.UUID) (*models.Object, error) {
	helper.SetupClient("localhost:" + port)
	return helper.GetObject(t, cls, uuid)
}
