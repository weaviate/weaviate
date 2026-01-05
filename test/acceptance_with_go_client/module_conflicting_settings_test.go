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

package acceptance_with_go_client

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestModuleConflictingSettings(t *testing.T) {
	envTestWeaviateImage := "TEST_WEAVIATE_IMAGE"
	currentWeaviateVersion := os.Getenv(envTestWeaviateImage)
	t.Run("text2vec-weaviate", func(t *testing.T) {
		t.Setenv(envTestWeaviateImage, "semitechnologies/weaviate:1.28.0")
		compose, err := docker.New().
			WithWeaviate().
			WithMockOIDCWithCertificate().
			Start(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, compose.Terminate(ctx))
		}()
	})
}
