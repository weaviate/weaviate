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
	"fmt"
	"math/rand"
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func clusterBackupJourneyTest(t *testing.T, backend, className,
	backupID, coordinatorEndpoint string, tenantNames []string, pqEnabled bool, override bool, overrideBucket, overrideLocation string,
	nodeEndpoints ...string,
) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)
	t.Logf("uploader selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	if len(tenantNames) > 0 {
		// upload data to a node other than the coordinator
		t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
			addTestClass(t, className, multiTenant)
			tenants := make([]*models.Tenant, len(tenantNames))
			for i := range tenantNames {
				tenants[i] = &models.Tenant{Name: tenantNames[i]}
			}
			helper.CreateTenants(t, className, tenants)
			addTestObjects(t, className, tenantNames)
		})
	} else {
		// upload data to a node other than the coordinator
		t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
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

	helper.SetupClient(coordinatorEndpoint)
	t.Logf("coordinator selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	// send backup requests to the chosen coordinator
	t.Run(fmt.Sprintf("with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourney(t, className, backend, backupID, clusterJourney,
			checkClassAndDataPresence, tenantNames, pqEnabled, map[string]string{}, override, overrideBucket, overrideLocation)
	})

	t.Run(fmt.Sprintf("cancelling with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourneyWithCancellation(t, className, backend, fmt.Sprintf("%s_with_cancellation", backupID), clusterJourney, overrideBucket, overrideLocation)
	})

	t.Run(fmt.Sprintf("listing backups with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourneyWithListing(t, clusterJourney, className, backend, backupID, overrideBucket, overrideLocation)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}

func clusterBackupEmptyClassJourneyTest(t *testing.T, backend, className, backupID,
	coordinatorEndpoint string, tenantNames []string, override bool, overrideBucket, overridePath string, nodeEndpoints ...string,
) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)
	t.Logf("uploader selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	if len(tenantNames) > 0 {
		// upload data to a node other than the coordinator
		t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
			addTestClass(t, className, multiTenant)
			tenants := make([]*models.Tenant, len(tenantNames))
			for i := range tenantNames {
				tenants[i] = &models.Tenant{Name: tenantNames[i]}
			}
			helper.CreateTenants(t, className, tenants)
		})
	} else {
		// upload data to a node other than the coordinator
		t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
			addTestClass(t, className, !multiTenant)
		})
	}

	helper.SetupClient(coordinatorEndpoint)
	t.Logf("coordinator selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	// send backup requests to the chosen coordinator
	t.Run(fmt.Sprintf("with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourney(t, className, backend, backupID, clusterJourney,
			checkClassPresenceOnly, tenantNames, false, map[string]string{}, false, "", "")
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}

func clusterNodeMappingBackupJourneyTest(t *testing.T, backend, className, backupID, coordinatorEndpoint string, override bool, overrideBucket, overridePath string, nodeEndpoints ...string) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)

	t.Logf("uploader selected -> %s:%s", helper.ServerHost, helper.ServerPort)
	t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
		addTestClass(t, className, !multiTenant)
	})

	// send backup requests to the chosen coordinator, with nodeMapping.
	// for nodeMapping we simply reverse the node(s) around, where node1 is now node2 and node2 is now node1.
	t.Run(fmt.Sprintf("with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourney(t, className, backend, backupID, clusterJourney, checkClassPresenceOnly, nil, false,
			map[string]string{"node1": "node2", "node2": "node1"}, override, overrideBucket, overridePath)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}
