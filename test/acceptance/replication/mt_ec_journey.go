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

package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func multiTenancyEventualConsistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.ContainerURI(1))
	paragraphClass := articles.ParagraphsClass()

	paragraphClass.ReplicationConfig = &models.ReplicationConfig{
		Factor: 3,
	}
	paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled: true,
	}
	helper.CreateClass(t, paragraphClass)

	t.Run("Execute journey", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			tenantName := fmt.Sprintf("tenant-%v", i)
			t.Run(fmt.Sprintf("InsertParagraphs/%v", tenantName), func(t *testing.T) {
				helper.CreateTenants(t, paragraphClass.Class, []*models.Tenant{{Name: tenantName, ActivityStatus: "HOT"}})
				objects := make([]*models.Object, 1000)
				for i := 0; i < 1000; i++ {
					objects[i] = (*models.Object)(articles.NewParagraph().
						WithContents(fmt.Sprintf("paragraph#%d", i)).
						WithTenant(tenantName).
						Object())
				}
				helper.CreateObjectsBatch(t, objects)
			})
		}
	})
}
