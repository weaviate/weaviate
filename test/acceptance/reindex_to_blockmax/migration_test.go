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

package reindex_to_blockmax

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const className = "ReindexTest"

// documents contains deterministic text for BM25 testing.
// Each document has unique terms to produce stable, predictable ranking.
var documents = []struct {
	text string
}{
	{text: "alpha bravo charlie delta echo foxtrot"},
	{text: "golf hotel india juliet kilo lima"},
	{text: "mike november oscar papa quebec romeo"},
	{text: "sierra tango uniform victor whiskey xray"},
	{text: "yankee zulu alpha bravo charlie delta"},
	{text: "echo foxtrot golf hotel india juliet"},
	{text: "kilo lima mike november oscar papa"},
	{text: "quebec romeo sierra tango uniform victor"},
	{text: "whiskey xray yankee zulu alpha bravo"},
	{text: "charlie delta echo foxtrot golf hotel"},
	{text: "india juliet kilo lima mike november"},
	{text: "oscar papa quebec romeo sierra tango"},
	{text: "uniform victor whiskey xray yankee zulu"},
	{text: "alpha charlie echo golf india kilo"},
	{text: "mike oscar quebec sierra uniform whiskey"},
	{text: "yankee bravo delta foxtrot hotel juliet"},
	{text: "lima november papa romeo tango victor"},
	{text: "xray zulu alpha echo india mike"},
	{text: "oscar sierra uniform yankee charlie foxtrot"},
	{text: "hotel kilo november quebec romeo victor"},
	{text: "alpha alpha alpha bravo bravo charlie"},
	{text: "delta delta delta echo echo foxtrot"},
	{text: "golf golf golf hotel hotel india"},
	{text: "juliet juliet juliet kilo kilo lima"},
	{text: "mike mike mike november november oscar"},
}

// bm25Queries are the queries we use to test BM25 stability.
var bm25Queries = []string{
	"alpha",
	"bravo charlie",
	"echo foxtrot golf",
	"mike november oscar",
}

func TestRuntimeMigrationToBlockmax(t *testing.T) {
	ctx := context.Background()

	// Start Weaviate with debug port exposed and non-BMW default.
	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	debugURI := "http://" + compose.GetWeaviate().DebugURI()

	// On test failure, dump container logs for debugging.
	defer func() {
		if t.Failed() {
			reader, err := compose.GetWeaviate().Container().Logs(ctx)
			if err != nil {
				t.Logf("failed to get container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			t.Logf("=== Container logs ===\n%s", string(logs))
		}
	}()

	// 1. Create a non-BMW collection with a text property.
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:         "text",
				DataType:     []string{"text"},
				Tokenization: "word",
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{
				K1: 1.2,
				B:  0.75,
			},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Verify UsingBlockMaxWAND is false.
	createdClass := helper.GetClass(t, className)
	require.False(t, createdClass.InvertedIndexConfig.UsingBlockMaxWAND,
		"collection should start with UsingBlockMaxWAND=false")

	// 2. Import objects.
	for i, doc := range documents {
		obj := &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"text": doc.text,
			},
		}
		err := helper.CreateObject(t, obj)
		require.NoError(t, err, "failed to create object %d", i)
	}

	// 3. Run baseline BM25 queries and record expected results.
	type queryResult struct {
		query string
		ids   []string
	}
	baselines := make([]queryResult, len(bm25Queries))
	for i, q := range bm25Queries {
		ids := runBM25Query(t, q)
		require.NotEmpty(t, ids, "baseline query %q returned no results", q)
		baselines[i] = queryResult{query: q, ids: ids}
	}
	t.Logf("baseline queries recorded: %d queries", len(baselines))

	// 4. Start background query loop.
	var (
		queryFailures atomic.Int64
		queryRuns     atomic.Int64
		stopCh        = make(chan struct{})
		wg            sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			for _, bl := range baselines {
				ids, err := runBM25QuerySafe(t, bl.query)
				queryRuns.Add(1)
				if err != nil {
					queryFailures.Add(1)
					t.Logf("query %q error during migration: %v", bl.query, err)
				} else if !idsMatch(bl.ids, ids) {
					queryFailures.Add(1)
					t.Logf("query %q mismatch during migration: expected %v, got %v",
						bl.query, bl.ids, ids)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 5. Trigger migration via debug endpoint.
	migrationURL := fmt.Sprintf("%s/debug/reindex/to-blockmax?collection=%s", debugURI, className)
	t.Logf("triggering migration: %s", migrationURL)

	resp, err := http.Get(migrationURL) //nolint:gosec
	require.NoError(t, err, "migration HTTP request failed")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "reading migration response body")
	t.Logf("migration response (status=%d): %s", resp.StatusCode, string(body))
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"migration endpoint returned non-200: %s", string(body))

	// 6. Stop background query loop.
	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "BM25 queries failed during migration")

	// 7. Verify schema: UsingBlockMaxWAND should now be true.
	updatedClass := helper.GetClass(t, className)
	t.Logf("post-migration InvertedIndexConfig: %+v", updatedClass.InvertedIndexConfig)
	require.True(t, updatedClass.InvertedIndexConfig.UsingBlockMaxWAND,
		"UsingBlockMaxWAND should be true after migration")

	// 8. Final BM25 queries — must return the same set of results.
	// The order may differ slightly for documents with identical BM25 scores
	// because BlockmaxWAND and the old WAND use different tie-breaking.
	for _, bl := range baselines {
		ids := runBM25Query(t, bl.query)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-migration query %q results differ from baseline", bl.query)
	}

	// 9. Extract shard name from migration response.
	var migrationResult map[string]map[string]string
	require.NoError(t, json.Unmarshal(body, &migrationResult))
	var shardName string
	for k := range migrationResult {
		shardName = k
		break
	}
	require.NotEmpty(t, shardName, "could not extract shard name from migration response")

	// 10. Verify no leftover suffixed bucket directories.
	container := compose.GetWeaviate().Container()
	dirs := listLSMDirs(ctx, t, container, className, shardName)
	assertNoSuffixedBuckets(t, dirs, "__blockmax_")

	// 11. Restart the server.
	t.Log("restarting weaviate container")
	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())

	// 12. Verify queries return correct results after restart.
	for _, bl := range baselines {
		ids := runBM25Query(t, bl.query)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-restart query %q results differ from baseline", bl.query)
	}

	// 13. Verify filesystem still clean after restart.
	dirs = listLSMDirs(ctx, t, container, className, shardName)
	assertNoSuffixedBuckets(t, dirs, "__blockmax_")
}

// runBM25Query executes a BM25 query and returns the ordered list of object IDs.
// It calls t.Fatal on error, so must only be called from the test goroutine.
func runBM25Query(t *testing.T, query string) []string {
	t.Helper()
	ids, err := runBM25QuerySafe(t, query)
	require.NoError(t, err, "BM25 query %q failed", query)
	return ids
}

// runBM25QuerySafe executes a BM25 query and returns the ordered list of object IDs.
// It returns an error instead of calling t.Fatal, making it safe for goroutines.
func runBM25QuerySafe(t *testing.T, query string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["text"]}) {
				text
				_additional { id }
			}
		}
	}`, className, query)

	resp, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", gqlQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", resp.Errors[0].Message)
	}

	data := make(map[string]interface{})
	for key, value := range resp.Data {
		data[key] = value
	}
	getMap := data["Get"].(map[string]interface{})
	items := getMap[className].([]interface{})

	ids := make([]string, 0, len(items))
	for _, item := range items {
		m := item.(map[string]interface{})
		additional := m["_additional"].(map[string]interface{})
		ids = append(ids, additional["id"].(string))
	}
	return ids, nil
}

// listLSMDirs lists directory names under the shard's LSM path inside the container.
func listLSMDirs(ctx context.Context, t *testing.T, c testcontainers.Container, col, shard string) []string {
	t.Helper()
	path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(col), shard)
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", path})
	require.NoError(t, err, "exec ls on container")
	require.Equal(t, 0, code, "ls returned non-zero exit code")
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	require.NoError(t, err)
	var dirs []string
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			dirs = append(dirs, line)
		}
	}
	return dirs
}

// assertNoSuffixedBuckets checks that no LSM directory names contain the given suffix.
func assertNoSuffixedBuckets(t *testing.T, dirs []string, suffix string) {
	t.Helper()
	for _, d := range dirs {
		assert.False(t, strings.Contains(d, suffix),
			"unexpected leftover bucket directory: %s", d)
	}
}

// idsMatch compares two slices of IDs for equality (order matters).
func idsMatch(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
