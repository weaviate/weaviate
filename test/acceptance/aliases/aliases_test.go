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

package test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestAliases boots a single Weaviate container shared across the REST, gRPC and
// backup suites. Each suite uses a unique alias-name prefix (Rest/Grpc/Backup)
// and cleans up after itself so the instance-wide alias counts stay exact.
func TestAliases(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviateWithGRPC().
		WithText2VecModel2Vec().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("rest", func(t *testing.T) {
		testAliasesAPI(t)
	})
	t.Run("grpc", func(t *testing.T) {
		testAliasesAPIgRPC(t, compose.GetWeaviate().GrpcURI())
	})
	t.Run("backup", func(t *testing.T) {
		testAliasesAPIBackup(t)
	})
}

// countAliasesWithPrefix returns how many instance-wide aliases start with the
// given prefix. Each suite scopes its count assertions to its own prefix so a
// sibling suite's aliases can't throw the numbers off.
func countAliasesWithPrefix(t *testing.T, prefix string) int {
	resp := helper.GetAliases(t, nil)
	require.NotNil(t, resp)
	count := 0
	for _, alias := range resp.Aliases {
		if strings.HasPrefix(alias.Alias, prefix) {
			count++
		}
	}
	return count
}
