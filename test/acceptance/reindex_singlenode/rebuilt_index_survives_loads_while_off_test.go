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

package reindex_singlenode

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	indexOffClass     = "IndexOffShardLoadTest"
	indexOffTenant    = "tenantone"
	indexOffProp      = "score"
	indexOffCanonical = "property_score"
	indexOffStaged    = "property_score__enable_filterable_ingest"
	indexOffObjects   = 25
)

// Deleting an index while its tenant is inactive leaves the shard in the same
// state as the promotion-deferral window: the rebuilt data must survive
// repeated shard loads under its staged name until the index is on again.
func TestRebuiltIndexSurvivesShardLoadsWhileTheIndexIsOff(t *testing.T) {
	ctx := context.Background()

	compose, err := reindexhelpers.StartSingleNode(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()

	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: indexOffClass,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}, Tokenization: "field"},
			{
				Name:              indexOffProp,
				DataType:          []string{"int"},
				IndexFilterable:   &falseVal,
				IndexRangeFilters: &falseVal,
			},
		},
		Vectorizer:         "none",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	})
	helper.CreateTenants(t, indexOffClass, []*models.Tenant{{Name: indexOffTenant}})

	objects := make([]*models.Object, indexOffObjects)
	for i := range objects {
		objects[i] = &models.Object{
			Class:  indexOffClass,
			Tenant: indexOffTenant,
			Properties: map[string]interface{}{
				"name":       fmt.Sprintf("item_%d", i),
				indexOffProp: int64(i % 5),
			},
		}
	}
	helper.CreateObjectsBatch(t, objects)

	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, indexOffClass, indexOffProp, "filterable", `{}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireIndexOffFilterableFlag(t, true)

	dirs := listLSMDirs(ctx, t, container, indexOffClass, indexOffTenant)
	require.NotEmpty(t, indexOffStagedDir(dirs),
		"fixture: the finished migration leaves its data under a staged name, got %v", dirs)

	// The delete only reaches loaded shards, so an inactive tenant keeps the
	// rebuilt index on disk after the flag turns off.
	setIndexOffTenantStatus(t, models.TenantActivityStatusINACTIVE)
	deleteIndex(t, restURI, indexOffClass, indexOffProp, "filterable")
	requireIndexOffFilterableFlag(t, false)

	for load := 1; load <= 2; load++ {
		setIndexOffTenantStatus(t, models.TenantActivityStatusACTIVE)
		require.Eventually(t, func() bool {
			ids, err := indexOffTenantObjects(t)
			return err == nil && len(ids) == indexOffObjects
		}, 60*time.Second, 200*time.Millisecond,
			"load %d: activating the tenant must load its shard and serve its objects", load)

		dirs := listLSMDirs(ctx, t, container, indexOffClass, indexOffTenant)
		assert.NotContainsf(t, dirs, indexOffCanonical,
			"load %d: the canonical directory must stay absent while the class turns the index off — "+
				"the next load's sweep deletes what is there", load)
		assert.NotEmptyf(t, indexOffStagedDir(dirs),
			"load %d: the rebuilt index must keep its staged name until the flag lands, got %v", load, dirs)

		setIndexOffTenantStatus(t, models.TenantActivityStatusINACTIVE)
	}
}

func indexOffStagedDir(dirs []string) string {
	for _, dir := range dirs {
		if strings.HasPrefix(dir, indexOffStaged) {
			return dir
		}
	}
	return ""
}

func indexOffTenantObjects(t *testing.T) ([]string, error) {
	t.Helper()
	return runGraphQLQuery(t, indexOffClass, fmt.Sprintf(`{
		Get {
			%s(tenant: %q) {
				name
				_additional { id }
			}
		}
	}`, indexOffClass, indexOffTenant))
}

func requireIndexOffFilterableFlag(t *testing.T, want bool) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		cls := helper.GetClass(t, indexOffClass)
		if cls == nil {
			return false
		}
		for _, prop := range cls.Properties {
			if prop.Name == indexOffProp {
				return prop.IndexFilterable != nil && *prop.IndexFilterable == want
			}
		}
		return false
	}, 60*time.Second, 50*time.Millisecond, "%s.IndexFilterable must settle on %v", indexOffProp, want)
}

// Waits for the status to land: the delete and the shard load that follow both
// read whether the shard is loaded, not what was asked for.
func setIndexOffTenantStatus(t *testing.T, status string) {
	t.Helper()
	switch status {
	case models.TenantActivityStatusACTIVE:
		helper.ActivateTenants(t, indexOffClass, []string{indexOffTenant})
	default:
		helper.DeactivateTenants(t, indexOffClass, []string{indexOffTenant})
	}
	settled := map[string]string{
		models.TenantActivityStatusACTIVE:   models.TenantActivityStatusHOT,
		models.TenantActivityStatusINACTIVE: models.TenantActivityStatusCOLD,
	}[status]
	require.Eventuallyf(t, func() bool {
		resp, err := helper.GetOneTenant(t, indexOffClass, indexOffTenant)
		if err != nil || resp == nil || resp.Payload == nil {
			return false
		}
		return resp.Payload.ActivityStatus == status || resp.Payload.ActivityStatus == settled
	}, 60*time.Second, 100*time.Millisecond, "tenant must reach %s", status)
}
