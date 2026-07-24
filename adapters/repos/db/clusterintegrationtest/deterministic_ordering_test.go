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

//go:build integrationTest

package clusterintegrationtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const deterministicClass = "DeterministicOrdering"

// TestDeterministicOrderingForEqualDistances verifies that distributed vector search
// returns deterministic ordering when multiple results have identical distances.
//
// This test reproduces the flaky test scenario from issue #11609:
// - Distributed search across multiple shards
// - Results with intentionally equal distances
// - Repeated searches must return exact same ordering
func TestDeterministicOrderingForEqualDistances(t *testing.T) {
	dirName := t.TempDir()

	// Create a multi-node setup (3 nodes/shards for this test)
	const numNodes = 3
	var nodes []*node

	overallShardState := multiShardState(numNodes)
	for i := 0; i < numNodes; i++ {
		n := &node{name: fmt.Sprintf("node-%d", i)}
		nodes = append(nodes, n)
	}

	for _, n := range nodes {
		n.init(t, dirName, &nodes, overallShardState, false)
	}

	// Apply schema
	class := deterministicOrderingClass()
	for i := range nodes {
		err := nodes[i].migrator.AddClass(context.Background(), class)
		require.NoError(t, err)
		nodes[i].schemaManager.schema.Objects.Classes = append(
			nodes[i].schemaManager.schema.Objects.Classes, class)
	}

	// Create objects with IDENTICAL vectors - this guarantees equal distances
	// We use different IDs so tie-breaking by ID should produce deterministic order
	identicalVector := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}

	// Create 6 objects with the same vector, distributed across shards
	// Using UUIDs that sort in a known order: aaaa < bbbb < cccc < dddd < eeee < ffff
	objectIDs := []strfmt.UUID{
		strfmt.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
		strfmt.UUID("bbbbbbbb-0000-0000-0000-000000000002"),
		strfmt.UUID("cccccccc-0000-0000-0000-000000000003"),
		strfmt.UUID("dddddddd-0000-0000-0000-000000000004"),
		strfmt.UUID("eeeeeeee-0000-0000-0000-000000000005"),
		strfmt.UUID("ffffffff-0000-0000-0000-000000000006"),
	}

	// Insert objects, distributing across nodes
	for i, id := range objectIDs {
		obj := &models.Object{
			ID:    id,
			Class: deterministicClass,
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("object-%d", i),
			},
			Vector: identicalVector,
		}
		// Distribute across nodes in round-robin fashion
		node := nodes[i%numNodes]
		err := node.repo.PutObject(context.Background(), obj, obj.Vector, nil, nil, nil, 0)
		require.NoError(t, err)
	}

	// Wait for indexing
	for _, n := range nodes {
		time.Sleep(50 * time.Millisecond)
		n.repo.GetScheduler().Schedule(context.Background())
		_ = n.repo.GetScheduler().WaitAll(t.Context())
	}

	// Query vector - same as stored vectors, so all have distance 0
	queryVector := identicalVector

	// Run the same search multiple times from different nodes
	// All searches should return the same ordering
	const numSearches = 20

	var firstResultOrder []strfmt.UUID

	for searchNum := 0; searchNum < numSearches; searchNum++ {
		// Pick a random node for each search to exercise different coordinator paths
		node := nodes[searchNum%numNodes]

		res, err := node.repo.VectorSearch(context.Background(), dto.GetParams{
			Pagination: &filters.Pagination{Limit: 10},
			ClassName:  deterministicClass,
		}, []string{""}, []models.Vector{queryVector})
		require.NoError(t, err)
		require.Len(t, res, len(objectIDs), "should return all objects")

		// Extract result IDs
		resultIDs := make([]strfmt.UUID, len(res))
		for i, r := range res {
			resultIDs[i] = r.ID
		}

		if searchNum == 0 {
			// Record the first result order
			firstResultOrder = resultIDs

			// Verify the expected ordering: sorted by ID ascending (tie-breaker)
			expectedOrder := []strfmt.UUID{
				strfmt.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
				strfmt.UUID("bbbbbbbb-0000-0000-0000-000000000002"),
				strfmt.UUID("cccccccc-0000-0000-0000-000000000003"),
				strfmt.UUID("dddddddd-0000-0000-0000-000000000004"),
				strfmt.UUID("eeeeeeee-0000-0000-0000-000000000005"),
				strfmt.UUID("ffffffff-0000-0000-0000-000000000006"),
			}
			assert.Equal(t, expectedOrder, resultIDs,
				"first search should return IDs sorted by ID (tie-breaker for equal distances)")
		} else {
			// All subsequent searches must return the same order
			assert.Equal(t, firstResultOrder, resultIDs,
				"search %d: ordering must be deterministic across repeated searches", searchNum)
		}
	}

	// Cleanup
	for _, n := range nodes {
		n.repo.Shutdown(context.Background())
	}
}

// TestDeterministicOrderingMixedDistances verifies that when some results have
// equal distances and some have different distances, the ordering is still deterministic.
func TestDeterministicOrderingMixedDistances(t *testing.T) {
	dirName := t.TempDir()

	const numNodes = 3
	var nodes []*node

	overallShardState := multiShardState(numNodes)
	for i := 0; i < numNodes; i++ {
		n := &node{name: fmt.Sprintf("node-%d", i)}
		nodes = append(nodes, n)
	}

	for _, n := range nodes {
		n.init(t, dirName, &nodes, overallShardState, false)
	}

	class := deterministicOrderingClass()
	for i := range nodes {
		err := nodes[i].migrator.AddClass(context.Background(), class)
		require.NoError(t, err)
		nodes[i].schemaManager.schema.Objects.Classes = append(
			nodes[i].schemaManager.schema.Objects.Classes, class)
	}

	// Create objects with intentional distance groupings:
	// - Group 1: 2 objects at distance ~0.0 (same as query)
	// - Group 2: 2 objects at distance ~0.1
	// - Group 3: 2 objects at distance ~0.2
	queryVector := []float32{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}

	objects := []struct {
		id     strfmt.UUID
		vector []float32
	}{
		// Group 1: distance ~0 (tied)
		{strfmt.UUID("bbbbbbbb-1111-1111-1111-111111111111"), []float32{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}},
		{strfmt.UUID("aaaaaaaa-1111-1111-1111-111111111111"), []float32{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}},
		// Group 2: distance ~0.1 (tied)
		{strfmt.UUID("dddddddd-2222-2222-2222-222222222222"), []float32{0.95, 0.31, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}},
		{strfmt.UUID("cccccccc-2222-2222-2222-222222222222"), []float32{0.95, 0.31, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}},
		// Group 3: distance ~0.2 (tied)
		{strfmt.UUID("ffffffff-3333-3333-3333-333333333333"), []float32{0.9, 0.44, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}},
		{strfmt.UUID("eeeeeeee-3333-3333-3333-333333333333"), []float32{0.9, 0.44, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}},
	}

	for i, obj := range objects {
		o := &models.Object{
			ID:    obj.id,
			Class: deterministicClass,
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("object-%d", i),
			},
			Vector: obj.vector,
		}
		node := nodes[i%numNodes]
		err := node.repo.PutObject(context.Background(), o, o.Vector, nil, nil, nil, 0)
		require.NoError(t, err)
	}

	for _, n := range nodes {
		time.Sleep(50 * time.Millisecond)
		n.repo.GetScheduler().Schedule(context.Background())
		_ = n.repo.GetScheduler().WaitAll(t.Context())
	}

	const numSearches = 20
	var firstResultOrder []strfmt.UUID

	for searchNum := 0; searchNum < numSearches; searchNum++ {
		node := nodes[searchNum%numNodes]

		res, err := node.repo.VectorSearch(context.Background(), dto.GetParams{
			Pagination: &filters.Pagination{Limit: 10},
			ClassName:  deterministicClass,
		}, []string{""}, []models.Vector{queryVector})
		require.NoError(t, err)
		require.Len(t, res, len(objects))

		resultIDs := make([]strfmt.UUID, len(res))
		for i, r := range res {
			resultIDs[i] = r.ID
		}

		if searchNum == 0 {
			firstResultOrder = resultIDs

			// Expected order: sorted by distance, then by ID within each distance group
			expectedOrder := []strfmt.UUID{
				// Group 1 (distance ~0): sorted by ID
				strfmt.UUID("aaaaaaaa-1111-1111-1111-111111111111"),
				strfmt.UUID("bbbbbbbb-1111-1111-1111-111111111111"),
				// Group 2 (distance ~0.1): sorted by ID
				strfmt.UUID("cccccccc-2222-2222-2222-222222222222"),
				strfmt.UUID("dddddddd-2222-2222-2222-222222222222"),
				// Group 3 (distance ~0.2): sorted by ID
				strfmt.UUID("eeeeeeee-3333-3333-3333-333333333333"),
				strfmt.UUID("ffffffff-3333-3333-3333-333333333333"),
			}
			assert.Equal(t, expectedOrder, resultIDs,
				"results should be sorted by distance, then by ID for ties")
		} else {
			assert.Equal(t, firstResultOrder, resultIDs,
				"search %d: ordering must be deterministic", searchNum)
		}
	}

	for _, n := range nodes {
		n.repo.Shutdown(context.Background())
	}
}

// TestDeterministicOrderingMatchesBruteForce verifies that the distributed search
// ordering matches the brute-force ground truth ordering.
func TestDeterministicOrderingMatchesBruteForce(t *testing.T) {
	dirName := t.TempDir()

	const numNodes = 5
	var nodes []*node

	overallShardState := multiShardState(numNodes)
	for i := 0; i < numNodes; i++ {
		n := &node{name: fmt.Sprintf("node-%d", i)}
		nodes = append(nodes, n)
	}

	for _, n := range nodes {
		n.init(t, dirName, &nodes, overallShardState, false)
	}

	class := deterministicOrderingClass()
	for i := range nodes {
		err := nodes[i].migrator.AddClass(context.Background(), class)
		require.NoError(t, err)
		nodes[i].schemaManager.schema.Objects.Classes = append(
			nodes[i].schemaManager.schema.Objects.Classes, class)
	}

	// Create objects with identical vectors (guarantees equal distances)
	identicalVector := []float32{0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5}
	numObjects := 20

	var data []*models.Object
	for i := 0; i < numObjects; i++ {
		id := strfmt.UUID(uuid.New().String())
		obj := &models.Object{
			ID:    id,
			Class: deterministicClass,
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("object-%d", i),
			},
			Vector: identicalVector,
		}
		data = append(data, obj)

		node := nodes[i%numNodes]
		err := node.repo.PutObject(context.Background(), obj, obj.Vector, nil, nil, nil, 0)
		require.NoError(t, err)
	}

	for _, n := range nodes {
		time.Sleep(50 * time.Millisecond)
		n.repo.GetScheduler().Schedule(context.Background())
		_ = n.repo.GetScheduler().WaitAll(t.Context())
	}

	queryVector := identicalVector

	// Get brute-force ground truth (uses deterministic tie-breaking)
	groundTruth := bruteForceObjectsByQuery(data, queryVector)

	// Run distributed search multiple times
	const numSearches = 10
	for searchNum := 0; searchNum < numSearches; searchNum++ {
		node := nodes[searchNum%numNodes]

		res, err := node.repo.VectorSearch(context.Background(), dto.GetParams{
			Pagination: &filters.Pagination{Limit: numObjects},
			ClassName:  deterministicClass,
		}, []string{""}, []models.Vector{queryVector})
		require.NoError(t, err)
		require.Len(t, res, numObjects)

		// Verify ordering matches brute-force ground truth
		for i, obj := range res {
			assert.Equal(t, groundTruth[i].ID, obj.ID,
				"search %d, position %d: distributed search should match brute-force ordering", searchNum, i)
		}
	}

	for _, n := range nodes {
		n.repo.Shutdown(context.Background())
	}
}

func deterministicOrderingClass() *models.Class {
	cfg := enthnsw.NewDefaultUserConfig()
	cfg.EF = 500
	return &models.Class{
		Class:             deterministicClass,
		VectorIndexConfig: cfg,
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
}
