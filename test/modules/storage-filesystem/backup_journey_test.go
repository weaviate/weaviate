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

package test

import (
	"os"
	"testing"

	"github.com/semi-technologies/weaviate/test/helper/journey"
)

func Test_BackupJourney(t *testing.T) {
	t.Run("storage-filesystem", func(t *testing.T) {
		journey.BackupJourneyTests(t, os.Getenv(weaviateEndpoint),
			"filesystem", "FileSystemClass", "filesystem-snapshot-1")
	})
}
