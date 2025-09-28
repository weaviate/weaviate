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

package test_suits

import (
	"testing"

	"github.com/weaviate/weaviate/test/docker"
)

func AllWithRestart(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("named vectors", testNamedVectorsRestart(compose))
		t.Run("vector search", testCompresseVectorTypesRestart(compose))
	}
}
