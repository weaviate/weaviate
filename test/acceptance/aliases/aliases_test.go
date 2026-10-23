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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/test/helper/sample-schema/documents"
)

// TestAliases runs the REST, gRPC and backup suites against the shared
// server. Each suite uses a unique alias-name prefix (Rest/Grpc/Backup) and
// cleans up after itself so the instance-wide alias counts stay exact.
func TestAliases(t *testing.T) {
	helper.SetupClient(helper.SharedServerURI)
	defer helper.ResetClient()

	for _, class := range []string{books.DefaultClassName, "Books2", documents.Document, documents.Passage} {
		helper.DeleteClass(t, class)
	}

	t.Run("rest", func(t *testing.T) {
		testAliasesAPI(t)
	})
	t.Run("grpc", func(t *testing.T) {
		testAliasesAPIgRPC(t, helper.SharedServerGRPCURI)
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
