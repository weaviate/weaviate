//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package journey

import (
	"testing"
)

// BackupJourneyTests this method gathers all backup related e2e tests
func BackupJourneyTests(t *testing.T, weaviateEndpoint, backend, className, backupID string) {
	// This is a simple test which covers almost the same scenario as singleShardBackupJourneyTest
	// but is left here to be expanded in the future with a more complex example
	// like adding there a new reference property and trying to run the test with 2 classes which
	// one of those classes is a class with a reference property
	t.Run("backup and restore Books", func(t *testing.T) {
		backupAndRestoreJourneyTest(t, weaviateEndpoint, backend)
	})

	t.Run("single shard backup", func(t *testing.T) {
		singleShardBackupJourneyTest(t, weaviateEndpoint, backend, className, backupID)
	})
}
