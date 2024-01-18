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

package test

import (
	"os"
	"testing"

	"github.com/weaviate/weaviate/test/helper/journey"
)

func Test_WeaviateCluster_GroupBy(t *testing.T) {
	t.Run("multi node", func(t *testing.T) {
		journey.GroupBySingleAndMultiShardTests(t, os.Getenv(weaviateNode1Endpoint))
	})
}
