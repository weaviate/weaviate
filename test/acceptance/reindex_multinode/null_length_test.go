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

package reindex_multinode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// Enabling filterable on a property with no index, in a collection that
// indexes null state and property length, must leave IsNull and length
// filters answering like a collection that had the index from the start,
// for objects written before, during and after the run, and across a restart.
func TestMultiNode_EnableFilterableBuildsNullAndLengthIndexes(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const migrating, control = "NullLengthEnable", "NullLengthControl"
	uri := func() string { return compose.GetWeaviateNode(1).URI() }
	createNullLengthCollection(t, uri(), migrating, false)
	createNullLengthCollection(t, uri(), control, true)
	defer func() {
		deleteCollection(t, uri(), migrating)
		deleteCollection(t, uri(), control)
	}()

	next := 0
	write := func(n int) {
		for i := 0; i < n; i++ {
			for _, class := range []string{migrating, control} {
				putNullLengthObject(t, uri(), class, next)
			}
			next++
		}
	}
	write(30)

	taskID := reindexhelpers.SubmitIndexUpsert(t, uri(), migrating, "p", "filterable", `{}`)
	write(10)
	reindexhelpers.AwaitReindexFinished(t, uri(), taskID)
	require.Eventually(t, func() bool {
		class, ok := reindexhelpers.FetchClass(uri(), migrating, false)
		return ok && class.Properties[0].IndexFilterable != nil && *class.Properties[0].IndexFilterable
	}, 60*time.Second, 100*time.Millisecond, "the schema flip has to land")
	write(10)
	requireNullLengthMatches(t, compose, migrating, control, "after the run")

	restartCluster(ctx, t, compose)
	write(5)
	requireNullLengthMatches(t, compose, migrating, control, "after a restart")
}

func createNullLengthCollection(t *testing.T, restURI, className string, indexed bool) {
	t.Helper()
	prop := map[string]any{"name": "p", "dataType": []string{"text"}, "tokenization": "word"}
	if !indexed {
		prop["indexFilterable"], prop["indexSearchable"] = false, false
	}
	body, err := json.Marshal(map[string]any{
		"class": className, "vectorizer": "none",
		"invertedIndexConfig": map[string]any{"indexNullState": true, "indexPropertyLength": true},
		"shardingConfig":      map[string]any{"desiredCount": 3},
		"replicationConfig":   map[string]any{"factor": 2},
		"properties":          []any{prop},
	})
	require.NoError(t, err)
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/schema", restURI), "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create %s: %s", className, respBody)
}

// Every third object leaves p out and every fourth sets it empty.
func putNullLengthObject(t *testing.T, restURI, className string, i int) {
	t.Helper()
	props := map[string]any{}
	switch {
	case i%3 == 2:
	case i%4 == 0:
		props["p"] = ""
	default:
		props["p"] = []string{"a", "a b", "a b c"}[i%3]
	}
	body, err := json.Marshal(map[string]any{
		"class": className, "id": fmt.Sprintf("00000000-0000-0000-0000-%012d", i), "properties": props,
	})
	require.NoError(t, err)
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/objects?consistency_level=ALL", restURI),
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "write %d to %s: %s", i, className, respBody)
}

func nullLengthHits(t *testing.T, restURI, className, where string) int {
	t.Helper()
	query := fmt.Sprintf(`{ Get { %s(limit: 1000, where: %s) { _additional { id } } } }`, className, where)
	body, err := json.Marshal(map[string]string{"query": query})
	require.NoError(t, err)
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/graphql", restURI), "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Data struct {
			Get map[string][]any `json:"Get"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Empty(t, out.Errors, "%s on %s", where, className)
	return len(out.Data.Get[className])
}

func requireNullLengthMatches(t *testing.T, compose *docker.DockerCompose, migrating, control, when string) {
	t.Helper()
	for _, where := range []string{
		`{path: ["p"], operator: IsNull, valueBoolean: true}`,
		`{path: ["p"], operator: IsNull, valueBoolean: false}`,
		`{path: ["len(p)"], operator: Equal, valueInt: 0}`,
		`{path: ["len(p)"], operator: GreaterThan, valueInt: 1}`,
	} {
		for node := 1; node <= 3; node++ {
			restURI := compose.GetWeaviateNode(node).URI()
			want := nullLengthHits(t, restURI, control, where)
			require.NotZero(t, want, "%s: the control must match something for %s", when, where)
			require.Equal(t, want, nullLengthHits(t, restURI, migrating, where), "%s: node %d, %s", when, node, where)
		}
	}
}
