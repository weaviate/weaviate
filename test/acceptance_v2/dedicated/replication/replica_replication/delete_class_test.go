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

package replica_replication

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestReplicationDeletingClassCleansUpOperations(t *testing.T) {
	t.Parallel()
	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With3NodeCluster()
	})
	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()

	stateToDeleteIn := []api.ShardReplicationState{
		api.REGISTERED,
		api.HYDRATING,
		api.FINALIZING,
	}

	for _, state := range stateToDeleteIn {
		t.Run("create schema", func(t *testing.T) {
			helper.DeleteClassWithClient(t, client, paragraphClass.Class)
			helper.CreateClassWithClient(t, client, paragraphClass)
		})

		t.Run("insert paragraphs", func(t *testing.T) {
			batch := make([]*models.Object, 10000)
			for i := 0; i < 10000; i++ {
				batch[i] = articles.NewParagraph().
					WithContents(fmt.Sprintf("paragraph#%d", i)).
					Object()
			}
			helper.CreateObjectsBatch(t, client, batch)
		})

		var id strfmt.UUID
		t.Run("create replication operation", func(t *testing.T) {
			created, err := client.Replication.Replicate(replication.NewReplicateParams().WithBody(getRequest(t, client, paragraphClass.Class)), nil)
			require.Nil(t, err)
			require.NotNil(t, created)
			require.NotNil(t, created.Payload)
			require.NotNil(t, created.Payload.ID)
			id = *created.Payload.ID
		})

		if state != api.REGISTERED {
			t.Run(fmt.Sprintf("wait until op is in %s state", state), func(t *testing.T) {
				assert.EventuallyWithT(t, func(ct *assert.CollectT) {
					details, err := client.Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
					require.Nil(ct, err)
					require.Equal(ct, state.String(), details.Payload.Status.State)
				}, 60*time.Second, 100*time.Millisecond, "replication operation should be in %s state", state)
			})
		}

		t.Run("delete class", func(t *testing.T) {
			helper.DeleteClassWithClient(t, client, paragraphClass.Class)
		})

		t.Run("wait for replication operation to be deleted", func(t *testing.T) {
			assert.EventuallyWithT(t, func(ct *assert.CollectT) {
				_, err := client.Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
				require.NotNil(ct, err)
				assert.IsType(ct, replication.NewReplicationDetailsNotFound(), err)
			}, 30*time.Second, 1*time.Second, "replication operation should be deleted")
		})
	}
}
