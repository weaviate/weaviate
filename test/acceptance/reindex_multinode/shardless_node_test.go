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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_ShardlessNodeBlockmaxFromRAFT pins that per-property blockmax
// truth is derived from RAFT-consistent state, not node-local shard buckets:
// with a 1-shard/RF1 class, a shardless node must agree with the shard-holder
// on both a migrated and an unmigrated property.
func TestMultiNode_ShardlessNodeBlockmaxFromRAFT(t *testing.T) {
	ctx := context.Background()
	// Boot on WAND so change-algorithm has real map→blockmax work to do.
	compose, cleanup := start3NodeReindexCluster(ctx, t, "USE_INVERTED_SEARCHABLE", "false")
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "ShardlessBlockmax"
	submitURI := compose.GetWeaviateNode(1).URI()

	// 1 shard, RF1 → a single node owns the shard; two nodes are shardless.
	createCollection(t, compose, submitURI, className, 1, 1, textProps("title", "body"))
	defer deleteCollection(t, submitURI, className)

	importTitleBody(t, submitURI, className, testDocuments)

	shardlessURI, shardHolderURI := findShardlessNodeURI(t, compose, submitURI, className)
	t.Logf("shard-holder=%s shardless=%s", shardHolderURI, shardlessURI)

	// Migrate only "title"; "body" stays WAND so the class-wide flip stays deferred.
	taskID := reindexhelpers.SubmitIndexUpsert(t, submitURI, className, "title", "searchable", `{"algorithm":"blockmax"}`)
	reindexhelpers.AwaitReindexViaIndexes(t, submitURI, className, "title", "searchable", reindexhelpers.WithTimeout(180*time.Second))
	reindexhelpers.AwaitReindexFinished(t, submitURI, taskID, reindexhelpers.WithTimeout(180*time.Second))

	// Class flag must stay false everywhere (including shardless nodes) while
	// "body" is still on WAND.
	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		require.NotNil(t, cls.InvertedIndexConfig)
		assert.False(t, cls.InvertedIndexConfig.UsingBlockMaxWAND,
			"node %d: class flag must stay deferred while 'body' is on WAND", i)
	}

	// Shardless node must derive title=blockmax, body=WAND from the RAFT task
	// list (no local bucket to probe).
	for _, tc := range []struct {
		label string
		uri   string
	}{
		{"shardless node", shardlessURI},
		{"shard-holder node", shardHolderURI},
	} {
		t.Run(tc.label, func(t *testing.T) {
			assert.Equal(t, "blockmax", searchableAlgorithm(t, tc.uri, className, "title"),
				"title migrated → blockmax")
			assert.Equal(t, "wand", searchableAlgorithm(t, tc.uri, className, "body"),
				"body untouched → WAND")

			// Repeat PUT blockmax on the migrated property is idempotent: 200 NO_OP.
			resp := reindexhelpers.SubmitIndexUpsertRaw(t, tc.uri, className, "title", "searchable", `{"algorithm":"blockmax"}`)
			require.Equal(t, http.StatusOK, resp.StatusCode, "repeat blockmax PUT must NO_OP: %s", resp.Body)
			assert.Contains(t, resp.Body, "NO_OP")

			// Rebuild is allowed because title is blockmax (not WAND) per RAFT.
			rebuildTaskID := reindexhelpers.RebuildIndex(t, tc.uri, className, "title", "searchable")
			reindexhelpers.AwaitReindexFinished(t, tc.uri, rebuildTaskID, reindexhelpers.WithTimeout(180*time.Second))

			// Rebuild on the still-WAND property must be rejected (migrate first).
			wandRebuild := reindexhelpers.RebuildIndexRaw(t, tc.uri, className, "body", "searchable")
			assert.Equal(t, http.StatusBadRequest, wandRebuild.StatusCode,
				"rebuild on a WAND property must 400: %s", wandRebuild.Body)
		})
	}
}

// searchableAlgorithm returns the algorithm ("wand"/"blockmax") the GET
// /indexes endpoint reports for a property's searchable index on the given node.
func searchableAlgorithm(t *testing.T, restURI, className, propName string) string {
	t.Helper()
	indexes := reindexhelpers.GetIndexes(t, restURI, className)
	for _, prop := range indexes.Properties {
		if prop.Name != propName {
			continue
		}
		for _, idx := range prop.Indexes {
			if idx.Type == "searchable" {
				return idx.Algorithm
			}
		}
	}
	t.Fatalf("no searchable index found for property %q on %s", propName, restURI)
	return ""
}

// findShardlessNodeURI returns the REST URI of a node that holds NO shard of
// className and one that DOES. With a 1-shard RF1 class on a 3-node cluster,
// exactly one node owns the shard and two do not. Node names are weaviate-N
// (CLUSTER_HOSTNAME); GetWeaviateNode(n) maps to container weaviate-{n-1}.
func findShardlessNodeURI(t *testing.T, compose *docker.DockerCompose, restURI, className string) (shardless, shardHolder string) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/nodes?output=verbose", restURI))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "nodes verbose failed: %s", string(body))

	var nodesResp struct {
		Nodes []struct {
			Name   string `json:"name"`
			Shards []struct {
				Class string `json:"class"`
			} `json:"shards"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(body, &nodesResp))

	uriForName := func(name string) string {
		var idx int
		_, err := fmt.Sscanf(name, "weaviate-%d", &idx)
		require.NoErrorf(t, err, "unexpected node name %q", name)
		return compose.GetWeaviateNode(idx + 1).URI()
	}

	for _, node := range nodesResp.Nodes {
		holds := false
		for _, sh := range node.Shards {
			if sh.Class == className {
				holds = true
				break
			}
		}
		if holds {
			shardHolder = uriForName(node.Name)
		} else if shardless == "" {
			shardless = uriForName(node.Name)
		}
	}
	require.NotEmpty(t, shardless, "expected a node holding no shard of %s", className)
	require.NotEmpty(t, shardHolder, "expected a node holding the shard of %s", className)
	return shardless, shardHolder
}

// importTitleBody imports objects setting both searchable text properties, at
// consistency ALL so the single replica has applied before the migration runs.
func importTitleBody(t *testing.T, restURI, className string, texts []string) {
	t.Helper()
	for i, text := range texts {
		obj := map[string]interface{}{
			"class":      className,
			"properties": map[string]interface{}{"title": text, "body": text},
		}
		body, err := json.Marshal(obj)
		require.NoError(t, err)
		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/objects?consistency_level=ALL", restURI),
			"application/json", bytes.NewReader(body))
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equalf(t, http.StatusOK, resp.StatusCode, "import object %d failed: %s", i, string(respBody))
	}
}
