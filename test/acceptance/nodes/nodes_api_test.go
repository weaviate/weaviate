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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/meta"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/test/helper/sample-schema/documents"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multishard"
)

func Test_NodesAPI(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecContextionary().
		WithWeaviateEnv("PERSISTENCE_MAX_REUSE_WAL_SIZE", "0").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "2"). // flush fast enough so object counts are correct
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("empty DB", func(t *testing.T) {
		meta, err := helper.Client(t).Meta.MetaGet(meta.NewMetaGetParams(), nil)
		require.Nil(t, err)
		assert.NotNil(t, meta.GetPayload())

		assertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
			require.NotNil(t, nodeStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
			assert.True(t, len(nodeStatus.Name) > 0)
			assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
			assert.Equal(t, meta.Payload.Version, nodeStatus.Version)
			assert.Empty(t, nodeStatus.Shards)
			require.Nil(t, nodeStatus.Stats)
		}

		testStatusResponse(t, assertions, nil, "")
	})

	t.Run("DB with Books (1 class ,1 shard configuration, 1 node)", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}

		minimalAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
			require.NotNil(t, nodeStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
			assert.True(t, len(nodeStatus.Name) > 0)
			assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
		}

		verboseAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
			require.Len(t, nodeStatus.Shards, 1)
			shard := nodeStatus.Shards[0]
			assert.True(t, len(shard.Name) > 0)
			assert.Equal(t, booksClass.Class, shard.Class)
			assert.Equal(t, int64(3), shard.ObjectCount)
			assert.Equal(t, int64(1), shard.ReplicationFactor)
			assert.Equal(t, int64(1), shard.NumberOfReplicas)
			require.NotNil(t, nodeStatus.Stats)
			assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
			assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
		}

		testStatusResponse(t, minimalAssertions, verboseAssertions, "")
	})

	t.Run("DB with MultiShard (1 class, 2 shards configuration, 1 node)", func(t *testing.T) {
		multiShardClass := multishard.ClassContextionaryVectorizer()
		helper.CreateClass(t, multiShardClass)
		defer helper.DeleteClass(t, multiShardClass.Class)

		for _, multiShard := range multishard.Objects() {
			helper.CreateObject(t, multiShard)
			helper.AssertGetObjectEventually(t, multiShard.Class, multiShard.ID)
		}

		minimalAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
			require.NotNil(t, nodeStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
			assert.True(t, len(nodeStatus.Name) > 0)
			assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
		}

		verboseAsssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
			assert.Len(t, nodeStatus.Shards, 2)
			for _, shard := range nodeStatus.Shards {
				assert.True(t, len(shard.Name) > 0)
				assert.Equal(t, multiShardClass.Class, shard.Class)
				assert.GreaterOrEqual(t, shard.ObjectCount, int64(0))
				assert.Equal(t, int64(1), shard.ReplicationFactor)
				assert.Equal(t, int64(1), shard.NumberOfReplicas)
				require.NotNil(t, nodeStatus.Stats)
				assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
				assert.Equal(t, int64(2), nodeStatus.Stats.ShardCount)
			}
		}

		testStatusResponse(t, minimalAssertions, verboseAsssertions, "")
	})

	t.Run("with class name: DB with Books and Documents, 1 shard, 1 node", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("insert and check books", func(t *testing.T) {
			for _, book := range books.Objects() {
				helper.CreateObject(t, book)
				helper.AssertGetObjectEventually(t, book.Class, book.ID)
			}

			minimalAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {}
			verboseAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
				require.NotNil(t, nodeStatus.Stats)
				assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
				assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
			}

			testStatusResponse(t, minimalAssertions, verboseAssertions, "")
		})

		t.Run("insert and check documents", func(t *testing.T) {
			docsClasses := documents.ClassesContextionaryVectorizer(false)
			helper.CreateClass(t, docsClasses[0])
			helper.CreateClass(t, docsClasses[1])
			defer helper.DeleteClass(t, docsClasses[0].Class)
			defer helper.DeleteClass(t, docsClasses[1].Class)

			for _, doc := range documents.Objects() {
				helper.CreateObject(t, doc)
				helper.AssertGetObjectEventually(t, doc.Class, doc.ID)
			}

			docsClass := docsClasses[0]

			minimalAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
				assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
				assert.True(t, len(nodeStatus.Name) > 0)
				assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
			}

			verboseAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
				require.NotNil(t, nodeStatus.Stats)
				assert.Equal(t, int64(2), nodeStatus.Stats.ObjectCount)
				assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
				assert.Len(t, nodeStatus.Shards, 1)
				shard := nodeStatus.Shards[0]
				assert.True(t, len(shard.Name) > 0)
				assert.Equal(t, docsClass.Class, shard.Class)
				assert.Equal(t, int64(2), shard.ObjectCount)
				assert.Equal(t, int64(1), shard.ReplicationFactor)
				assert.Equal(t, int64(1), shard.NumberOfReplicas)
			}

			testStatusResponse(t, minimalAssertions, verboseAssertions, docsClass.Class)
		})
	})

	// This test prevents a regression of
	// https://github.com/weaviate/weaviate/issues/2454
	t.Run("validate count with updates", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		_, err := helper.BatchClient(t).BatchObjectsCreate(
			batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
				Objects: []*models.Object{
					{
						ID:    strfmt.UUID("2D0D3E3B-54B2-48D4-BFE0-4BE2C060110E"),
						Class: booksClass.Class,
						Properties: map[string]interface{}{
							"title":       "A book that changes",
							"description": "First iteration",
						},
					},
				},
			}), nil)
		require.Nil(t, err)

		// Note that this is the same ID as before, so this is an update!!
		_, err = helper.BatchClient(t).BatchObjectsCreate(
			batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
				Objects: []*models.Object{
					{
						ID:    strfmt.UUID("2D0D3E3B-54B2-48D4-BFE0-4BE2C060110E"),
						Class: booksClass.Class,
						Properties: map[string]interface{}{
							"title":       "A book that changes",
							"description": "A new (second) iteration",
						},
					},
				},
			}), nil)
		require.Nil(t, err)

		minimalAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {}
		verboseAssertions := func(t require.TestingT, nodeStatus *models.NodeStatus) {
			require.NotNil(t, nodeStatus.Stats)
			assert.Equal(t, int64(1), nodeStatus.Stats.ObjectCount)
		}

		testStatusResponse(t, minimalAssertions, verboseAssertions, "")
	})

	t.Run("verbose status racing a class delete", func(t *testing.T) {
		const (
			doomedTenants   = 20
			survivorTenants = 5
			pollers         = 4
		)

		doomed := createMultiTenantClass(t, "NodesRaceDoomed", doomedTenants)
		survivor := createMultiTenantClass(t, "NodesRaceSurvivor", survivorTenants)
		defer helper.DeleteClass(t, survivor.Class)
		// best effort: the test deletes this class itself, this only cleans up
		// after an early failure
		defer helper.DeleteClassObject(t, doomed.Class)

		// wait for both collections to be fully reported before racing them
		require.EventuallyWithT(t, func(t *assert.CollectT) {
			counts, err := shardsPerClass(doomed.Class, survivor.Class)
			require.NoError(t, err)
			assert.Equal(t, doomedTenants, counts[doomed.Class])
			assert.Equal(t, survivorTenants, counts[survivor.Class])
		}, 30*time.Second, 200*time.Millisecond)

		// pollProblems lists everything a single poll got wrong. The pollers
		// collect these instead of asserting, because a failed assertion from a
		// non-test goroutine is not reported reliably.
		pollProblems := func(counts map[string]int, err error) []string {
			if err != nil {
				return []string{fmt.Sprintf("node status failed: %v", err)}
			}
			var found []string
			if n := counts[survivor.Class]; n != survivorTenants {
				found = append(found, fmt.Sprintf(
					"untouched collection reported %d of %d shards", n, survivorTenants))
			}
			if n := counts[doomed.Class]; n != 0 && n != doomedTenants {
				found = append(found, fmt.Sprintf(
					"deleted collection reported %d shards, want 0 or %d", n, doomedTenants))
			}
			return found
		}

		var (
			mu       sync.Mutex
			problems []string
			polls    int
		)

		func() {
			var wg sync.WaitGroup
			stop := make(chan struct{})
			// the pollers must not outlive this scope, not even when the delete
			// below fails the test
			defer func() {
				close(stop)
				wg.Wait()
			}()

			for i := 0; i < pollers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}

						counts, err := shardsPerClass(doomed.Class, survivor.Class)
						found := pollProblems(counts, err)

						mu.Lock()
						polls++
						problems = append(problems, found...)
						mu.Unlock()
					}
				}()
			}

			helper.DeleteClass(t, doomed.Class)
		}()

		assert.Empty(t, problems)
		assert.Positive(t, polls, "the race never polled the nodes API")

		counts, err := shardsPerClass(doomed.Class, survivor.Class)
		require.NoError(t, err)
		assert.Zero(t, counts[doomed.Class], "deleted collection still reports shards")
		assert.Equal(t, survivorTenants, counts[survivor.Class])
	})
}

// createMultiTenantClass creates a class with the given number of HOT tenants,
// so every tenant shows up as its own shard in the verbose node status.
func createMultiTenantClass(t *testing.T, name string, tenantCount int) *models.Class {
	t.Helper()

	class := &models.Class{
		Class:              name,
		Vectorizer:         "none",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	helper.CreateClass(t, class)

	tenants := make([]*models.Tenant, tenantCount)
	for i := range tenants {
		tenants[i] = &models.Tenant{
			Name:           fmt.Sprintf("%s-tenant-%d", name, i),
			ActivityStatus: models.TenantActivityStatusHOT,
		}
	}
	helper.CreateTenants(t, name, tenants)
	return class
}

// shardsPerClass counts the shards the verbose node status reports for each of
// the given classes. It returns errors instead of failing the test, so callers
// can poll it from their own goroutines.
func shardsPerClass(classes ...string) (map[string]int, error) {
	output := verbosity.OutputVerbose
	params := nodes.NewNodesGetParams().WithOutput(&output)
	body, err := helper.Client(nil).Nodes.NodesGet(params, nil)
	if err != nil {
		return nil, err
	}
	if body.Payload == nil {
		return nil, errors.New("node status response has no payload")
	}
	if len(body.Payload.Nodes) != 1 {
		return nil, fmt.Errorf("node status reported %d nodes, want 1", len(body.Payload.Nodes))
	}

	counts := make(map[string]int, len(classes))
	for _, class := range classes {
		counts[class] = 0
	}
	for _, shard := range body.Payload.Nodes[0].Shards {
		if _, ok := counts[shard.Class]; ok {
			counts[shard.Class]++
		}
	}
	return counts, nil
}

func TestNodesApi_Compression_AsyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecContextionary().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_INDEXING_STALE_TIMEOUT", "500ms").
		WithWeaviateEnv("QUEUE_SCHEDULER_INTERVAL", "100ms").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("validate flat compression status", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		booksClass.VectorIndexType = "flat"
		booksClass.VectorIndexConfig = map[string]interface{}{
			"bq": map[string]interface{}{
				"enabled": true,
			},
		}
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("check compressed true", func(t *testing.T) {
			checkThunk := func() interface{} {
				verbose := "verbose"
				params := nodes.NewNodesGetParams().WithOutput(&verbose)
				resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
				require.Nil(t, err)

				nodeStatusResp := resp.GetPayload()
				require.NotNil(t, nodeStatusResp)

				nodes := nodeStatusResp.Nodes
				require.NotNil(t, nodes)
				require.Len(t, nodes, 1)

				nodeStatus := nodes[0]
				require.NotNil(t, nodeStatus)
				return nodeStatus.Shards[0].Compressed
			}

			helper.AssertEventuallyEqualWithFrequencyAndTimeout(t, true, checkThunk, 100*time.Millisecond, 15*time.Second)
		})
	})

	t.Run("validate hnsw pq async compression", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		booksClass.VectorIndexConfig = map[string]interface{}{
			"pq": map[string]interface{}{
				"trainingLimit": 256,
				"enabled":       true,
				"segments":      1,
			},
		}
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("check compressed initially false", func(t *testing.T) {
			verbose := "verbose"
			params := nodes.NewNodesGetParams().WithOutput(&verbose)
			resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
			require.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(t, nodeStatusResp)

			nodes := nodeStatusResp.Nodes
			require.NotNil(t, nodes)
			require.Len(t, nodes, 1)

			nodeStatus := nodes[0]
			require.NotNil(t, nodeStatus)

			require.False(t, nodeStatus.Shards[0].Compressed)
		})

		t.Run("load data for pq", func(t *testing.T) {
			num := 1024
			objects := make([]*models.Object, num)

			for i := 0; i < num; i++ {
				objects[i] = &models.Object{
					Class:  booksClass.Class,
					Vector: []float32{float32(i % 32), float32(i), 3.0, 4.0},
				}
			}

			_, err := helper.BatchClient(t).BatchObjectsCreate(
				batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
					Objects: objects,
				},
				), nil)
			require.Nil(t, err)
		})

		t.Run("check eventually compressed if async enabled", func(t *testing.T) {
			checkThunk := func() interface{} {
				verbose := "verbose"
				params := nodes.NewNodesGetParams().WithOutput(&verbose)
				resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
				require.Nil(t, err)

				nodeStatusResp := resp.GetPayload()
				require.NotNil(t, nodeStatusResp)

				nodes := nodeStatusResp.Nodes
				require.NotNil(t, nodes)
				require.Len(t, nodes, 1)

				nodeStatus := nodes[0]
				require.NotNil(t, nodeStatus)
				return nodeStatus.Shards[0].Compressed
			}

			helper.AssertEventuallyEqualWithFrequencyAndTimeout(t, true, checkThunk, 100*time.Millisecond, 15*time.Second)
		})
	})
}

func TestNodesApi_Compression_SyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecContextionary().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())
	t.Run("validate flat compression status", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		booksClass.VectorIndexType = "flat"
		booksClass.VectorIndexConfig = map[string]interface{}{
			"bq": map[string]interface{}{
				"enabled": true,
			},
		}
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("check compressed true", func(t *testing.T) {
			checkThunk := func() interface{} {
				verbose := "verbose"
				params := nodes.NewNodesGetParams().WithOutput(&verbose)
				resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
				require.Nil(t, err)

				nodeStatusResp := resp.GetPayload()
				require.NotNil(t, nodeStatusResp)

				nodes := nodeStatusResp.Nodes
				require.NotNil(t, nodes)
				require.Len(t, nodes, 1)

				nodeStatus := nodes[0]
				require.NotNil(t, nodeStatus)
				return nodeStatus.Shards[0].Compressed
			}

			helper.AssertEventuallyEqualWithFrequencyAndTimeout(t, true, checkThunk, 100*time.Millisecond, 10*time.Second)
		})
	})
}

func testStatusResponse(t *testing.T, minimalAssertions, verboseAssertions func(require.TestingT, *models.NodeStatus),
	class string,
) {
	minimal, verbose := verbosity.OutputMinimal, verbosity.OutputVerbose

	// the caller passes in the t to assert against: inside EventuallyWithT that
	// is the tick's CollectT, not the surrounding *testing.T
	commonTests := func(t require.TestingT, resp *nodes.NodesGetOK) {
		require.NotNil(t, resp.Payload)
		nodes := resp.Payload.Nodes
		require.NotNil(t, nodes)
		require.Len(t, nodes, 1)
		minimalAssertions(t, nodes[0])
	}

	t.Run("minimal", func(t *testing.T) {
		payload, err := getNodesStatus(t, minimal, class)
		require.NoError(t, err)
		commonTests(t, &nodes.NodesGetOK{Payload: payload})
	})

	if verboseAssertions != nil {
		t.Run("verbose", func(t *testing.T) {
			getNodes := func() (*models.NodesStatusResponse, error) {
				return getNodesStatus(t, verbose, class)
			}
			assert.EventuallyWithT(t, func(t *assert.CollectT) {
				payload, err := getNodes()
				require.NoError(t, err)
				commonTests(t, &nodes.NodesGetOK{Payload: payload})
				// If commonTests pass, resp.Nodes[0] != nil
				verboseAssertions(t, payload.Nodes[0])
			}, 15*time.Second, 500*time.Millisecond)
		})
	}
}

func getNodesStatus(t *testing.T, output, class string) (*models.NodesStatusResponse, error) {
	// the generated client returns a nil body along with the error, so the
	// payload can only be read once the error is ruled out
	if class != "" {
		params := nodes.NewNodesGetClassParams().WithOutput(&output).WithClassName(class)
		body, err := helper.Client(t).Nodes.NodesGetClass(params, nil)
		if err != nil {
			return nil, err
		}
		return body.Payload, nil
	}

	params := nodes.NewNodesGetParams().WithOutput(&output)
	body, err := helper.Client(t).Nodes.NodesGet(params, nil)
	if err != nil {
		return nil, err
	}
	return body.Payload, nil
}
