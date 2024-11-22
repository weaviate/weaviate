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

package journey

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func singleNodeBackupJourneyTest(t *testing.T,
	weaviateEndpoint, backend, className, backupID string,
	tenantNames []string, pqEnabled bool, overrideActive bool, overrideBucket, overridePath string,
) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}

	if len(tenantNames) > 0 {
		t.Run("add test data", func(t *testing.T) {
			addTestClass(t, className, multiTenant)
			tenants := make([]*models.Tenant, len(tenantNames))
			for i := range tenantNames {
				tenants[i] = &models.Tenant{Name: tenantNames[i]}
			}
			helper.CreateTenants(t, className, tenants)
			addTestObjects(t, className, tenantNames)
		})
	} else {
		t.Run("add test data", func(t *testing.T) {
			addTestClass(t, className, !multiTenant)
			addTestObjects(t, className, nil)
		})
	}

	if pqEnabled {
		pq := map[string]interface{}{
			"enabled":   true,
			"segments":  1,
			"centroids": 16,
		}
		helper.EnablePQ(t, className, pq)
	}

	t.Run("single node backup", func(t *testing.T) {
		backupJourney(t, className, backend, backupID, singleNodeJourney,
			checkClassAndDataPresence, tenantNames, pqEnabled, map[string]string{}, overrideActive, overrideBucket, overridePath)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}

func singleNodeBackupEmptyClassJourneyTest(t *testing.T,
	weaviateEndpoint, backend, className, backupID string,
	tenantNames []string, overrideActive bool, overrideBucket, overridePath string,
) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}

	if len(tenantNames) <= 0 {
		t.Skip("test is only relevant with tenancy enabled")
	}

	t.Run("add test class and tenant", func(t *testing.T) {
		addTestClass(t, className, multiTenant)
		tenants := make([]*models.Tenant, len(tenantNames))
		for i := range tenantNames {
			tenants[i] = &models.Tenant{Name: tenantNames[i]}
		}
		helper.CreateTenants(t, className, tenants)
	})

	t.Run("single node backup", func(t *testing.T) {
		backupJourney(t, className, backend, backupID,
			singleNodeJourney, checkClassPresenceOnly, tenantNames, false, map[string]string{}, overrideActive, overrideBucket, overridePath)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}
