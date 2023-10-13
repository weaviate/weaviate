//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package journey

import (
	"testing"
)

// BackupJourneyTests_SingleNode this method gathers all backup related e2e tests
func BackupJourneyTests_SingleNode(t *testing.T, weaviateEndpoint, backend, className, backupID string, tenantNames []string) {
	if len(tenantNames) > 0 {
		t.Run("multi-tenant single node backup", func(t *testing.T) {
			singleNodeBackupJourneyTest(t, weaviateEndpoint, backend, className, backupID, tenantNames)
		})
		t.Run("multi-tenant single node backup with empty class", func(t *testing.T) {
			singleNodeBackupEmptyClassJourneyTest(t, weaviateEndpoint, backend, className, backupID+"_empty", tenantNames)
		})
	} else {
		// This is a simple test which covers almost the same scenario as singleNodeBackupJourneyTest
		// but is left here to be expanded in the future with a more complex example
		// like adding there a new reference property and trying to run the test with 2 classes which
		// one of those classes is a class with a reference property
		t.Run("backup and restore Books", func(t *testing.T) {
			backupAndRestoreJourneyTest(t, weaviateEndpoint, backend)
		})

		t.Run("single-tenant single node backup", func(t *testing.T) {
			singleNodeBackupJourneyTest(t, weaviateEndpoint, backend, className, backupID, nil)
		})
	}
}

// BackupJourneyTests_Cluster this method gathers all backup related e2e tests to be run on a cluster
func BackupJourneyTests_Cluster(t *testing.T, backend, className, backupID string,
	tenantNames []string, weaviateEndpoints ...string,
) {
	if len(weaviateEndpoints) <= 1 {
		t.Fatal("must provide more than one node for cluster backup test")
	}

	if len(tenantNames) > 0 {
		t.Run("multi-tenant cluster backup", func(t *testing.T) {
			coordinator := weaviateEndpoints[0]
			clusterBackupJourneyTest(t, backend, className, backupID,
				coordinator, tenantNames, weaviateEndpoints[1:]...)
		})
		t.Run("multi-tenant cluster backup with empty class", func(t *testing.T) {
			coordinator := weaviateEndpoints[0]
			clusterBackupEmptyClassJourneyTest(t, backend, className, backupID+"_empty",
				coordinator, tenantNames, weaviateEndpoints[1:]...)
		})
	} else {
		t.Run("single-tenant cluster backup", func(t *testing.T) {
			if len(weaviateEndpoints) <= 1 {
				t.Fatal("must provide more than one node for cluster backup test")
			}

			coordinator := weaviateEndpoints[0]
			clusterBackupJourneyTest(t, backend, className, backupID,
				coordinator, nil, weaviateEndpoints[1:]...)
		})
	}
}
