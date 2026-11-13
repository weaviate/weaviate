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

package key_doc_column

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const (
	// indexedClass is named in INDEX_FILTERABLE_KEY_DOC_COLUMN; plainClass is the same collection without it.
	indexedClass = "Indexed"
	plainClass   = "Plain"
	numObjects   = 200
)

func skuOf(i int) string { return fmt.Sprintf("sku-%05d", i) }

func classWith(name string) *models.Class {
	return &models.Class{
		Class:      name,
		Vectorizer: "none",
		Properties: []*models.Property{{
			Name:     "sku",
			DataType: schema.DataTypeText.PropString(),
			// one value, one key — the only text shape a column can answer
			Tokenization: models.PropertyTokenizationField,
		}},
	}
}

// containsAny asks for the objects whose sku is one of values, through GraphQL.
func containsAny(t *testing.T, class string, values []string) []string {
	t.Helper()
	return containsQuery(t, class, "ContainsAny", values)
}

func containsNone(t *testing.T, class string, values []string) []string {
	t.Helper()
	return containsQuery(t, class, "ContainsNone", values)
}

func containsQuery(t *testing.T, class, operator string, values []string) []string {
	t.Helper()
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = fmt.Sprintf("%q", v)
	}
	query := fmt.Sprintf(`
		{
			Get {
				%s(limit: 1000, where: {
					path: ["sku"]
					operator: %s
					valueText: [%s]
				}) {
					sku
				}
			}
		}`, class, operator, strings.Join(quoted, ", "))

	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	got := result.Get("Get", class).AsSlice()

	out := make([]string, 0, len(got))
	for _, obj := range got {
		out = append(out, obj.(map[string]any)["sku"].(string))
	}
	sort.Strings(out)
	return out
}

// weaviateLogs is everything the node has said so far.
func weaviateLogs(t *testing.T, ctx context.Context, compose *docker.DockerCompose) string {
	t.Helper()
	reader, err := compose.GetWeaviate().Container().Logs(ctx)
	require.NoError(t, err)
	defer reader.Close()
	out, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(out)
}

// requireColumnServed fails unless the log names key_doc_column, printing the node's column lines when it does not.
func requireColumnServed(t *testing.T, logs, why string) {
	t.Helper()
	if strings.Contains(logs, "key_doc_column") {
		return
	}
	var relevant []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "keydoccolumn") ||
			strings.Contains(line, "contains_desugared") ||
			strings.Contains(line, "build_allow_list_doc_bitmap") ||
			strings.Contains(line, "slow query") {
			relevant = append(relevant, line)
		}
	}
	if len(relevant) > 10 {
		relevant = relevant[len(relevant)-10:]
	}
	t.Fatalf("%s\nwhat the node said about a column or a batched read:\n%s",
		why, strings.Join(relevant, "\n"))
}

// TestKeyDocColumnServesContainsFromAConfiguredProperty pins INDEX_FILTERABLE_KEY_DOC_COLUMN giving a column that answers, across a restart.
func TestKeyDocColumnServesContainsFromAConfiguredProperty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateEnv("INDEX_FILTERABLE_KEY_DOC_COLUMN", indexedClass+":sku").
		// log every query, so the log names which read answered; LogIfSlow reads 0 as unset, hence 1ns
		WithWeaviateEnv("QUERY_SLOW_LOG_ENABLED", "true").
		WithWeaviateEnv("QUERY_SLOW_LOG_THRESHOLD", "1ns").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	// no collection cleanup: the container is discarded with the test
	for _, class := range []string{indexedClass, plainClass} {
		helper.CreateClass(t, classWith(class))

		objects := make([]*models.Object, 0, numObjects)
		for i := 0; i < numObjects; i++ {
			objects = append(objects, &models.Object{
				Class:      class,
				Properties: map[string]any{"sku": skuOf(i)},
			})
		}
		helper.CreateObjectsBatch(t, objects)
	}

	asked := []string{skuOf(3), skuOf(7), skuOf(11)}

	assertAnswers := func(t *testing.T, when string) {
		t.Helper()

		indexed := containsAny(t, indexedClass, asked)
		assert.Equal(t, asked, indexed, "%s: ContainsAny answers exactly the values asked for", when)
		assert.Equal(t, containsAny(t, plainClass, asked), indexed,
			"%s: the configured collection must answer as the unconfigured one does", when)

		deniedIndexed := containsNone(t, indexedClass, asked)
		assert.Len(t, deniedIndexed, numObjects-len(asked), "%s: ContainsNone is the complement", when)
		assert.Equal(t, containsNone(t, plainClass, asked), deniedIndexed,
			"%s: and so must ContainsNone", when)
	}

	t.Run("while the data is still unflushed", func(t *testing.T) {
		assertAnswers(t, "unflushed")
		requireColumnServed(t, weaviateLogs(t, ctx, compose),
			"the configured property must be answered by its column, not by the fold")
	})

	t.Run("after a restart rebuilds the column from disk", func(t *testing.T) {
		seen := len(weaviateLogs(t, ctx, compose)) // docker restart keeps the log, so only later lines count
		require.NoError(t, compose.RestartAt(ctx, 0, nil))
		helper.SetupClient(compose.GetWeaviate().URI())

		assertAnswers(t, "after restart")
		requireColumnServed(t, weaviateLogs(t, ctx, compose)[seen:],
			"a column built at open must serve as one built from a flush does")
	})
}
