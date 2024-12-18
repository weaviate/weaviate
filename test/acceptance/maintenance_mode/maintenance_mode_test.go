//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package maintenance_mode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// TestMaintenanceMode starts a 3-node cluster and puts the 3rd node in
// maintenance mode. It then verifies that the 3rd node can still respond to
// schema/metadata changes/queries but not to object changes/queries.
func TestMaintenanceMode(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		WithWeaviateEnv("MAINTENANCE_NODES", "node3").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	testClass := articles.ParagraphsClass()
	testClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("create class", func(t *testing.T) {
		helper.CreateClass(t, testClass)
	})

	// The 3rd node is in maintenance mode but should still be able to respond
	// to schema/metadata changes/queries
	t.Run("verify class exists on node3", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviateNode3().URI())
		helper.GetClass(t, testClass.Class)
	})

	paragraphIDs := []strfmt.UUID{
		strfmt.UUID("3bf331ac-8c86-4f95-b127-2f8f96bbc093"),
		strfmt.UUID("47b26ba1-6bc9-41f8-a655-8b9a5b60e1a3"),
	}

	t.Run("Add objects with consistency level QUORUM succeeds", func(t *testing.T) {
		for idx, id := range paragraphIDs {
			o := articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", idx)).
				Object()
			helper.CreateObjectCL(t, o, "QUORUM")
		}
	})

	t.Run("Get objects with consistency level QUORUM succeeds", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviateNode2().URI())
		for idx, id := range paragraphIDs {
			res, err := helper.GetObjectCL(t, testClass.Class, id, "QUORUM")
			require.Nil(t, err)
			require.Equal(t, id, res.ID)
			require.Equal(t, helper.ObjectContentsProp(fmt.Sprintf("paragraph#%d", idx)), res.Properties)
		}
	})

	t.Run("Add objects with consistency level ALL should fail after timeout", func(t *testing.T) {
		t.Helper()
		o := articles.NewParagraph().
			WithID(strfmt.UUID("8833b60c-fe16-4b80-beed-440fc4738285")).
			WithContents(fmt.Sprintf("paragraph#%d", 42)).
			Object()
		cls := string("ALL")
		params := objects.NewObjectsCreateParamsWithTimeout(2 * time.Second).WithBody(o).WithConsistencyLevel(&cls)
		_, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
		require.NotNil(t, err, "expected error, got nil")
	})

	t.Run("Get objects with consistency level ALL should fail after timeout", func(t *testing.T) {
		cls := string("ALL")
		params := objects.NewObjectsClassGetParamsWithTimeout(2 * time.Second).WithID(paragraphIDs[0]).WithClassName(testClass.Class).WithConsistencyLevel(&cls)
		_, err = helper.Client(t).Objects.ObjectsClassGet(params, nil)
		require.NotNil(t, err, "expected error, got nil")
	})
}
