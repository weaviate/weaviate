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
	"fmt"
	"math/rand"
	"testing"

	"github.com/semi-technologies/weaviate/test/helper"
)

func clusterBackupJourneyTest(t *testing.T, backend, className, backupID, coordinatorEndpoint string, nodeEndpoints ...string) {
	uploaderEndpoint := nodeEndpoints[rand.Intn(len(nodeEndpoints))]
	helper.SetupClient(uploaderEndpoint)
	t.Logf("uploader selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	// upload data to a node other than the coordinator
	t.Run(fmt.Sprintf("add test data to endpoint: %s", uploaderEndpoint), func(t *testing.T) {
		addTestClass(t, className)
		addTestObjects(t, className)
	})

	helper.SetupClient(coordinatorEndpoint)
	t.Logf("coordinator selected -> %s:%s", helper.ServerHost, helper.ServerPort)

	// send backup requests to the chosen coordinator
	t.Run(fmt.Sprintf("with coordinator endpoint: %s", coordinatorEndpoint), func(t *testing.T) {
		backupJourney(t, className, backend, backupID)
	})

	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}
