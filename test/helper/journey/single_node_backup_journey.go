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

	"github.com/semi-technologies/weaviate/test/helper"
)

func singleNodeBackupJourneyTest(t *testing.T, weaviateEndpoint, backend, className, backupID string) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}

	t.Run("add test data", func(t *testing.T) {
		addTestClass(t, className)
		addTestObjects(t, className)
	})

	t.Run("single node backup", func(t *testing.T) {
		backupJourney(t, className, backend, backupID, singleNodeJourney)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}
