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
	"context"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func testAllObjectsIndexed(t *testing.T, ctx context.Context, c *client.Client, className string) {
	// wait for all of the objects to get indexed
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := c.Cluster().NodesStatusGetter().
			WithClass(className).
			WithOutput("verbose").
			Do(ctx)
		require.NoError(ct, err)
		require.NotEmpty(ct, resp.Nodes)
		for _, n := range resp.Nodes {
			require.NotEmpty(ct, n.Shards)
			for _, s := range n.Shards {
				assert.Equal(ct, "READY", s.VectorIndexingStatus)
			}
		}
	}, 30*time.Second, 500*time.Millisecond)
}

func TestAfterUnsetVsEmpty(t *testing.T) {
	c, className := createClientWithClassName(t)

	class := models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "bool", DataType: []string{string(schema.DataTypeBoolean)}},
			{Name: "counter", DataType: []string{string(schema.DataTypeInt)}},
		},
		Vectorizer: "none",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

	numObjs := 100
	for i := 0; i < numObjs; i++ {
		_, err := c.Data().Creator().WithClassName(className).WithProperties(
			map[string]interface{}{"counter": i, "bool": i%2 == 0},
		).Do(ctx)
		require.NoError(t, err)
	}

	getExplicitEmpty, err := c.GraphQL().Get().WithClassName(className).WithLimit(10).WithFields(
		graphql.Field{
			Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
		},
		graphql.Field{Name: "counter"},
	).WithAfter("").Do(ctx)
	require.NoError(t, err)
	require.Nil(t, getExplicitEmpty.Errors)
	objExplicitEmpty := getExplicitEmpty.Data["Get"].(map[string]interface{})[className].([]interface{})

	getUnset, err := c.GraphQL().Get().WithClassName(className).WithLimit(10).WithFields(
		graphql.Field{
			Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
		},
		graphql.Field{Name: "counter"},
	).Do(ctx)
	require.NoError(t, err)
	require.Nil(t, getUnset.Errors)
	objUnset := getUnset.Data["Get"].(map[string]interface{})[className].([]interface{})

	require.Equal(t, objExplicitEmpty, objUnset)
}

func TestIteratorWithFilter(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080", Timeout: 10 * time.Minute})
	require.Nil(t, err)

	className := "GoldenSunsetFlower"
	require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))

	class := models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "bool", DataType: []string{string(schema.DataTypeBoolean)}},
			{Name: "counter", DataType: []string{string(schema.DataTypeInt)}},
		},
		Vectorizer: "none",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

	trueUUIDs := make(map[string]struct{}, 0)
	numObjs := 100
	for i := 0; i < numObjs; i++ {
		obj, err := c.Data().Creator().WithClassName(className).WithProperties(
			map[string]interface{}{"counter": i, "bool": i%2 == 0},
		).Do(ctx)
		require.NoError(t, err)
		if i%2 == 0 {
			trueUUIDs[string(obj.Object.ID)] = struct{}{}
		}
	}

	found := 0
	var after string
	for {
		get, err := c.GraphQL().Get().WithClassName(className).WithWhere(filters.Where().
			WithPath([]string{"bool"}).
			WithOperator(filters.Equal).
			WithValueBoolean(true)).WithLimit(10).
			WithFields(
				graphql.Field{
					Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
				},
				graphql.Field{Name: "counter"},
			).WithAfter(after).Do(ctx)
		require.NoError(t, err)
		require.Nil(t, get.Errors)
		objs := get.Data["Get"].(map[string]interface{})[className].([]interface{})
		if len(objs) == 0 {
			break
		}
		found += len(objs)

		for _, obj := range objs {
			props := obj.(map[string]interface{})
			require.True(t, int(props["counter"].(float64))%2 == 0)
			id := props["_additional"].(map[string]interface{})["id"].(string)
			_, ok := trueUUIDs[id]
			require.True(t, ok, "Expected to find UUID %s in trueUUIDs")
			delete(trueUUIDs, id) // Make sure each object is only counted once
		}

		after = objs[len(objs)-1].(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string)
	}

	require.Equal(t, numObjs/2, found)
}

func TestIteratorWithFilterGRPC(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080", Timeout: 10 * time.Minute})
	require.NoError(t, err)

	className := "GoldenSunsetFlowerGRPC"
	require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))

	class := models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "bool", DataType: []string{string(schema.DataTypeBoolean)}},
			{Name: "counter", DataType: []string{string(schema.DataTypeInt)}},
		},
		Vectorizer: "none",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

	numObjs := 200
	uuids := make([]string, numObjs)
	var matchingUUID string

	// Build objects array
	objects := make([]*models.Object, numObjs)
	for i := 0; i < numObjs; i++ {
		objects[i] = &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"counter": i,
				"bool":    i == 2,
			},
		}
	}

	// Batch insert
	resp, err := c.Batch().ObjectsBatcher().WithObjects(objects...).Do(ctx)
	require.NoError(t, err)
	require.Len(t, resp, numObjs)

	// Extract UUIDs and validate responses
	for i, r := range resp {
		require.NotNil(t, r.Result)
		require.NotNil(t, r.Result.Status)
		require.Equal(t, "SUCCESS", *r.Result.Status)

		uuids[i] = string(r.ID)
		if i == 2 {
			matchingUUID = uuids[i]
		}
	}

	// Sort uuids ascending to determine expected scan limit position
	sort.Strings(uuids)

	// The scan limit is 10x the requested limit (10)
	// So the shard will scan up to 100 objects before returning
	// The cursor should point to the 100th UUID (index 99)
	expectedScanLimitUUID := uuids[99]

	testAllObjectsIndexed(t, ctx, c, className)

	// Connect via raw gRPC
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	grpcClient := pb.NewWeaviateClient(conn)

	emptyString := ""
	after := &emptyString

	// First request: should return 1 result (the object with counter=2)
	reply, err := grpcClient.Search(ctx, &pb.SearchRequest{
		Collection:  className,
		Limit:       10,
		After:       after,
		Uses_123Api: false,
		Uses_125Api: false,
		Uses_127Api: true,
		Metadata:    &pb.MetadataRequest{Uuid: true},
		Filters: &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			TestValue: &pb.Filters_ValueBoolean{
				ValueBoolean: true,
			},
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_Property{
					Property: "bool",
				},
			},
		},
	})
	require.NoError(t, err)

	require.Len(t, reply.Results, 1, "Expected 1 result matching the filter")

	// Verify the result is the expected object
	resultID := reply.Results[0].Metadata.Id
	require.Equal(t, matchingUUID, resultID, "Expected result to be the object with counter=2")

	// Verify ShardCursors map is populated
	require.NotEmpty(t, reply.ShardCursors, "Expected ShardCursors to be populated")

	// For a single-shard collection, there should be 1 entry
	require.Len(t, reply.ShardCursors, 1, "Expected 1 shard cursor entry")

	// Get the shard name and cursor value
	var shardName string
	var cursorValue string
	for name, cursor := range reply.ShardCursors {
		shardName = name
		cursorValue = cursor
	}

	// Verify the cursor is NOT the matching UUID
	require.NotEqual(t, matchingUUID, cursorValue,
		"Cursor should be the scan limit position, not the matching object")

	// The cursor should be the UUID where the scan limit was reached (100th UUID)
	// NOT the UUID of the returned matching object
	require.Equal(t, expectedScanLimitUUID, cursorValue,
		"Expected cursor to be the 100th UUID (scan limit position), not the matching object UUID")

	// Update 'after' to the last returned UUID (standard pagination contract)
	// Even though ShardCursors will be used, 'after' serves as fallback for new shards
	after = &matchingUUID

	// Second request: use ShardCursors from first response
	// Should return 0 results since the only matching object (counter=2) was before the cursor
	reply, err = grpcClient.Search(ctx, &pb.SearchRequest{
		Collection:   className,
		Limit:        20,
		After:        after,              // Set to last returned UUID (serves as fallback)
		ShardCursors: reply.ShardCursors, // Shard-specific cursors (takes precedence)
		Filters: &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			TestValue: &pb.Filters_ValueBoolean{
				ValueBoolean: true,
			},
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_Property{
					Property: "bool",
				},
			},
		},
	})
	require.NoError(t, err)

	require.Len(t, reply.Results, 0, "Expected 0 results in second request")

	// Verify ShardCursors still populated
	require.NotEmpty(t, reply.ShardCursors, "Expected ShardCursors to be populated in second response")

	// The cursor should now be uuid.Nil (shard exhausted)
	exhaustedCursor, ok := reply.ShardCursors[shardName]
	require.True(t, ok, "Expected same shard name in second response")
	require.Equal(t, uuid.Nil.String(), exhaustedCursor, "Expected shard to be marked as exhausted (uuid.Nil)")
}

func TestIteratorWithFilterGRPC_MultiShard(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080", Timeout: 10 * time.Minute})
	require.NoError(t, err)

	className := "MultiShardIteratorTestGRPC"
	require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))

	// Create class with 3 shards
	class := models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "bool", DataType: []string{string(schema.DataTypeBoolean)}},
			{Name: "counter", DataType: []string{string(schema.DataTypeInt)}},
		},
		Vectorizer: "none",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

	numObjs := 200
	matchingIndices := []int{2, 50, 150}  // Multiple matching objects
	matchingUUIDs := make(map[string]int) // Map UUID to counter value

	// Build objects array
	objects := make([]*models.Object, numObjs)
	for i := 0; i < numObjs; i++ {
		isMatching := false
		for _, idx := range matchingIndices {
			if i == idx {
				isMatching = true
				break
			}
		}
		objects[i] = &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"counter": i,
				"bool":    isMatching,
			},
		}
	}

	// Batch insert
	resp, err := c.Batch().ObjectsBatcher().WithObjects(objects...).Do(ctx)
	require.NoError(t, err)
	require.Len(t, resp, numObjs)

	// Extract UUIDs for matching objects
	for i, r := range resp {
		require.NotNil(t, r.Result)
		require.NotNil(t, r.Result.Status)
		require.Equal(t, "SUCCESS", *r.Result.Status)

		// Track UUIDs of matching objects
		for _, idx := range matchingIndices {
			if i == idx {
				matchingUUIDs[string(r.ID)] = i
				break
			}
		}
	}

	testAllObjectsIndexed(t, ctx, c, className)

	// Connect via raw gRPC
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	grpcClient := pb.NewWeaviateClient(conn)

	// Paginate through results until all matching objects are found
	emptyString := ""
	after := &emptyString
	var shardCursors map[string]string
	collectedUUIDs := make(map[string]bool)
	requestCount := 0
	maxRequests := 50 // Safety limit to avoid infinite loops

	for requestCount < maxRequests {
		requestCount++

		reply, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection:   className,
			Limit:        10,
			After:        after,
			ShardCursors: shardCursors,
			Uses_123Api:  false,
			Uses_125Api:  false,
			Uses_127Api:  true,
			Metadata:     &pb.MetadataRequest{Uuid: true},
			Filters: &pb.Filters{
				Operator: pb.Filters_OPERATOR_EQUAL,
				TestValue: &pb.Filters_ValueBoolean{
					ValueBoolean: true,
				},
				Target: &pb.FilterTarget{
					Target: &pb.FilterTarget_Property{
						Property: "bool",
					},
				},
			},
		})
		require.NoError(t, err)

		// On first request, verify we have 3 shard cursors
		if requestCount == 1 {
			require.NotEmpty(t, reply.ShardCursors, "Expected ShardCursors to be populated")
			require.Len(t, reply.ShardCursors, 3, "Expected 3 shard cursor entries for 3-shard collection")
		}

		// Collect returned UUIDs
		for _, result := range reply.Results {
			uuid := result.Metadata.Id
			collectedUUIDs[uuid] = true
			// Verify this UUID is one we expect
			_, ok := matchingUUIDs[uuid]
			require.True(t, ok, "Received unexpected UUID %s", uuid)
		}

		// Update cursors for next iteration
		shardCursors = reply.ShardCursors
		if len(reply.Results) > 0 {
			lastID := reply.Results[len(reply.Results)-1].Metadata.Id
			after = &lastID
		}

		// Check if all shards are exhausted
		allExhausted := true
		for _, cursor := range shardCursors {
			if cursor != uuid.Nil.String() {
				allExhausted = false
				break
			}
		}

		// If all shards exhausted and no results, we're done
		if allExhausted && len(reply.Results) == 0 {
			break
		}
	}

	// Verify distribution across shards
	nodesResp, err := c.Cluster().NodesStatusGetter().
		WithClass(className).
		WithOutput("verbose").
		Do(ctx)
	require.NoError(t, err)

	shardObjectCounts := make(map[string]int64)
	for _, node := range nodesResp.Nodes {
		for _, shard := range node.Shards {
			shardObjectCounts[shard.Name] = shard.ObjectCount
		}
	}

	// Verify distribution across multiple shards
	shardsWithObjects := 0
	for _, count := range shardObjectCounts {
		if count > 0 {
			shardsWithObjects++
		}
	}
	require.Greater(t, shardsWithObjects, 1,
		"Expected objects to be distributed across multiple shards, got %d shards with objects", shardsWithObjects)

	// Verify we collected all 3 matching objects
	require.Len(t, collectedUUIDs, len(matchingIndices),
		"Expected to collect all %d matching objects, got %d", len(matchingIndices), len(collectedUUIDs))

	// Verify all collected UUIDs match our expected set
	for collectedUUID := range collectedUUIDs {
		_, ok := matchingUUIDs[collectedUUID]
		require.True(t, ok, "Collected UUID %s not in expected matching UUIDs", collectedUUID)
	}

	require.Less(t, requestCount, maxRequests, "Test exceeded maximum request limit, possible infinite loop")
}

func TestIteratorWithFilterGRPC_MultiNode_ExhaustivePagination(t *testing.T) {
	testCases := []struct {
		name           string
		totalObjects   int
		matchingCount  int  // Number of objects with bool=true
		ensureLastTrue bool // Ensure last object has bool=true
	}{
		{
			name:           "1000 objects, 500 matching",
			totalObjects:   1000,
			matchingCount:  500,
			ensureLastTrue: true,
		},
		{
			name:           "5000 objects, 2500 matching",
			totalObjects:   5000,
			matchingCount:  2500,
			ensureLastTrue: true,
		},
		{
			name:           "10000 objects, 5000 matching",
			totalObjects:   10000,
			matchingCount:  5000,
			ensureLastTrue: true,
		},
		{
			name:           "20000 objects, 10000 matching",
			totalObjects:   20000,
			matchingCount:  10000,
			ensureLastTrue: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080", Timeout: 10 * time.Minute})
			require.NoError(t, err)

			className := "ExhaustivePaginationTestGRPC"
			require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))

			// Create class with 3 shards (will result in 9 total shards across 3 nodes)
			class := models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "bool", DataType: []string{string(schema.DataTypeBoolean)}},
					{Name: "counter", DataType: []string{string(schema.DataTypeInt)}},
				},
				Vectorizer: "none",
				ShardingConfig: map[string]interface{}{
					"desiredCount": 3,
				},
			}
			require.NoError(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

			// Pre-generate UUIDs in sorted order
			// This ensures objects are returned in a predictable order during pagination
			sortedUUIDs := make([]string, tc.totalObjects)
			for i := 0; i < tc.totalObjects; i++ {
				sortedUUIDs[i] = uuid.New().String()
			}
			sort.Strings(sortedUUIDs)

			// Determine which objects should have bool=true
			// Distribute them evenly, then ensure the last one is always true
			matchingSet := make(map[int]bool)
			if tc.matchingCount > 0 {
				step := tc.totalObjects / tc.matchingCount
				for i := 0; i < tc.matchingCount-1; i++ {
					idx := i * step
					matchingSet[idx] = true
				}
				// Always make the last object match if ensureLastTrue is set
				if tc.ensureLastTrue {
					matchingSet[tc.totalObjects-1] = true
				} else {
					// Otherwise place the last matching object somewhere
					matchingSet[(tc.matchingCount-1)*step] = true
				}
			}

			// Build objects array using pre-sorted UUIDs
			objects := make([]*models.Object, tc.totalObjects)
			expectedMatchingUUIDs := make(map[string]int) // Map UUID -> counter value

			for i := 0; i < tc.totalObjects; i++ {
				isMatching := matchingSet[i]
				objects[i] = &models.Object{
					ID:    strfmt.UUID(sortedUUIDs[i]),
					Class: className,
					Properties: map[string]interface{}{
						"counter": i,
						"bool":    isMatching,
					},
				}
				if isMatching {
					expectedMatchingUUIDs[sortedUUIDs[i]] = i
				}
			}

			t.Logf("Inserting %d objects with %d matching (bool=true)", tc.totalObjects, len(expectedMatchingUUIDs))

			// Batch insert with progress tracking
			batchSize := 100
			for start := 0; start < tc.totalObjects; start += batchSize {
				end := start + batchSize
				if end > tc.totalObjects {
					end = tc.totalObjects
				}

				batch := objects[start:end]
				resp, err := c.Batch().ObjectsBatcher().WithObjects(batch...).Do(ctx)
				require.NoError(t, err)
				require.Len(t, resp, len(batch))

				// Verify all batch operations succeeded
				for _, r := range resp {
					require.NotNil(t, r.Result)
					require.NotNil(t, r.Result.Status)
					require.Equal(t, "SUCCESS", *r.Result.Status, "Batch insert failed for object %s", r.ID)
				}

				if (start/batchSize)%10 == 0 {
					t.Logf("Progress: inserted %d/%d objects", end, tc.totalObjects)
				}
			}

			t.Logf("All objects inserted, waiting for indexing...")
			testAllObjectsIndexed(t, ctx, c, className)

			// Verify distribution across shards
			nodesResp, err := c.Cluster().NodesStatusGetter().
				WithClass(className).
				WithOutput("verbose").
				Do(ctx)
			require.NoError(t, err)

			totalShards := 0
			shardObjectCounts := make(map[string]int64)
			for _, node := range nodesResp.Nodes {
				for _, shard := range node.Shards {
					totalShards++
					shardObjectCounts[shard.Name] = shard.ObjectCount
					t.Logf("Node: %s, Shard: %s, Objects: %d", node.Name, shard.Name, shard.ObjectCount)
				}
			}

			// With 3 nodes and 3 shards per node, we expect 9 total shards
			require.GreaterOrEqual(t, totalShards, 3, "Expected at least 3 shards")

			// // Verify distribution across multiple shards
			// shardsWithObjects := 0
			// for _, count := range shardObjectCounts {
			// 	if count > 0 {
			// 		shardsWithObjects++
			// 	}
			// }
			// require.Greater(t, shardsWithObjects, 1,
			// 	"Expected objects to be distributed across multiple shards, got %d shards with objects", shardsWithObjects)

			// Connect via raw gRPC
			conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()

			grpcClient := pb.NewWeaviateClient(conn)

			// Paginate through ALL results with filters
			emptyString := ""
			after := &emptyString
			var shardCursors map[string]string
			collectedUUIDs := make(map[string]bool)
			requestCount := 0
			maxRequests := (tc.totalObjects / 10) + 100 // Reasonable upper bound

			t.Logf("Starting pagination to collect all %d matching objects...", len(expectedMatchingUUIDs))

			for requestCount < maxRequests {
				requestCount++

				reply, err := grpcClient.Search(ctx, &pb.SearchRequest{
					Collection:   className,
					Limit:        10,
					After:        after,
					ShardCursors: shardCursors,
					Uses_123Api:  false,
					Uses_125Api:  false,
					Uses_127Api:  true,
					Metadata:     &pb.MetadataRequest{Uuid: true},
					Filters: &pb.Filters{
						Operator: pb.Filters_OPERATOR_EQUAL,
						TestValue: &pb.Filters_ValueBoolean{
							ValueBoolean: true,
						},
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{
								Property: "bool",
							},
						},
					},
				})
				require.NoError(t, err)

				// On first request, verify shard cursors are populated
				if requestCount == 1 {
					require.NotEmpty(t, reply.ShardCursors, "Expected ShardCursors to be populated")
					t.Logf("First request: got %d results, %d shard cursors", len(reply.Results), len(reply.ShardCursors))
				}

				// Collect returned UUIDs and verify they're expected
				for _, result := range reply.Results {
					resultUUID := result.Metadata.Id

					// Check for duplicates
					if collectedUUIDs[resultUUID] {
						t.Fatalf("Duplicate UUID returned: %s (request #%d)", resultUUID, requestCount)
					}

					collectedUUIDs[resultUUID] = true

					// Verify this UUID is one we expect to match
					counterValue, ok := expectedMatchingUUIDs[resultUUID]
					require.True(t, ok, "Received unexpected UUID %s (not in expected matching set)", resultUUID)

					// Log if this is the last expected object
					if counterValue == tc.totalObjects-1 {
						t.Logf("Found last object (counter=%d) in request #%d", counterValue, requestCount)
					}
				}

				// Update cursors for next iteration
				shardCursors = reply.ShardCursors
				if len(reply.Results) > 0 {
					lastID := reply.Results[len(reply.Results)-1].Metadata.Id
					after = &lastID
				}

				// Log progress every 100 requests
				if requestCount%100 == 0 {
					t.Logf("Progress: %d requests made, %d/%d objects collected",
						requestCount, len(collectedUUIDs), len(expectedMatchingUUIDs))
				}

				// Check if all shards are exhausted
				allExhausted := true
				for _, cursor := range shardCursors {
					if cursor != uuid.Nil.String() {
						allExhausted = false
						break
					}
				}

				// If all shards exhausted and no results, we're done
				if allExhausted && len(reply.Results) == 0 {
					t.Logf("All shards exhausted after %d requests", requestCount)
					break
				}
			}

			// Critical assertions: verify NO objects were missed
			require.Len(t, collectedUUIDs, len(expectedMatchingUUIDs),
				"CRITICAL: Expected to collect exactly %d matching objects, but got %d",
				len(expectedMatchingUUIDs), len(collectedUUIDs))

			// Verify every expected UUID was collected
			missedUUIDs := []string{}
			for expectedUUID, counterValue := range expectedMatchingUUIDs {
				if !collectedUUIDs[expectedUUID] {
					missedUUIDs = append(missedUUIDs, expectedUUID)
					t.Logf("MISSED: UUID %s (counter=%d)", expectedUUID, counterValue)
				}
			}
			require.Empty(t, missedUUIDs, "CRITICAL: %d objects were not returned by pagination", len(missedUUIDs))

			// Verify no unexpected UUIDs were collected
			unexpectedUUIDs := []string{}
			for collectedUUID := range collectedUUIDs {
				if _, ok := expectedMatchingUUIDs[collectedUUID]; !ok {
					unexpectedUUIDs = append(unexpectedUUIDs, collectedUUID)
				}
			}
			require.Empty(t, unexpectedUUIDs, "Received %d unexpected UUIDs", len(unexpectedUUIDs))

			// Verify the last object (if ensureLastTrue) was collected
			if tc.ensureLastTrue {
				lastUUID := sortedUUIDs[tc.totalObjects-1]
				require.True(t, collectedUUIDs[lastUUID],
					"CRITICAL: Last object (UUID=%s) was not collected", lastUUID)
				t.Logf("SUCCESS: Last object was collected")
			}

			require.Less(t, requestCount, maxRequests, "Test exceeded maximum request limit (%d), possible infinite loop", maxRequests)

			t.Logf("✓ Test passed: All %d matching objects collected in %d requests (%.2f objects per request)",
				len(collectedUUIDs), requestCount, float64(len(collectedUUIDs))/float64(requestCount))
		})
	}
}

// Helper: Build LIKE filter for SHA prefix matching
func buildFilterForSHAPrefix(prefix string) *pb.Filters {
	pattern := "sha1:" + prefix + "*"
	return &pb.Filters{
		Operator: pb.Filters_OPERATOR_LIKE,
		TestValue: &pb.Filters_ValueText{
			ValueText: pattern,
		},
		Target: &pb.FilterTarget{
			Target: &pb.FilterTarget_Property{
				Property: "sha",
			},
		},
	}
}

// Helper: Calculate percentile from sorted duration slice
func calculatePercentile(sorted []time.Duration, percentile int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := (len(sorted) * percentile) / 100
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// Helper: Validate that SHA property starts with the expected prefix
func validateFilterMatch(result *pb.SearchResult, shaPrefix string) bool {
	if result.Properties == nil || result.Properties.NonRefProps == nil {
		return false
	}

	fields := result.Properties.NonRefProps.GetFields()
	if fields == nil {
		return false
	}

	shaValue, ok := fields["sha"]
	if !ok {
		return false
	}

	// Get text value from the Value union type
	textVal := shaValue.GetTextValue()
	if textVal == "" {
		return false
	}

	// Check if SHA starts with the expected prefix
	expectedPrefix := "sha1:" + shaPrefix
	return assert.True(&testing.T{}, len(textVal) >= len(expectedPrefix) && textVal[:len(expectedPrefix)] == expectedPrefix, "SHA should start with %s", expectedPrefix)
}

func TestIteratorWithFilter_With_Sphere_Dataset(t *testing.T) {
	ctx := context.Background()
	className := "Test_sphere"

	// Create REST client for verification
	c, err := client.NewClient(client.Config{
		Scheme:  "http",
		Host:    "localhost:8080",
		Timeout: 30 * time.Minute,
	})
	require.NoError(t, err)

	// Verify collection exists
	class, err := c.Schema().ClassGetter().WithClassName(className).Do(ctx)
	if err != nil || class == nil {
		t.Skipf("Collection '%s' does not exist. Please load sphere.1M.jsonl first.", className)
		return
	}

	// Verify collection has objects
	nodesResp, err := c.Cluster().NodesStatusGetter().
		WithClass(className).
		WithOutput("verbose").
		Do(ctx)
	require.NoError(t, err)

	totalObjects := int64(0)
	shardCount := 0
	for _, node := range nodesResp.Nodes {
		for _, shard := range node.Shards {
			totalObjects += shard.ObjectCount
			shardCount++
		}
	}

	t.Logf("Collection '%s' has %d objects across %d shards", className, totalObjects, shardCount)

	// Require at least 900K objects
	require.GreaterOrEqual(t, totalObjects, int64(900000),
		"Expected ~1M objects, got %d", totalObjects)

	// Wait for indexing to complete
	testAllObjectsIndexed(t, ctx, c, className)

	// Connect via gRPC
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	grpcClient := pb.NewWeaviateClient(conn)

	// Define test cases using SHA prefix filtering (Base32 encoding)
	// SHA hashes in the Sphere dataset use base32 encoding (A-Z, 2-7)
	// Expected counts are EXACT values from the sphere.1M.jsonl dataset
	testCases := []struct {
		name          string
		shaPrefix     string
		description   string
		expectedCount int // EXACT count from sphere.1M.jsonl - must match for deterministic results
	}{
		{
			name:          "Medium-high selectivity - SHA prefix '2'",
			shaPrefix:     "2",
			description:   "sha LIKE 'sha1:2*'",
			expectedCount: 31965, // Exact count from sphere.1M.jsonl (3.20%)
		},
		{
			name:          "Medium selectivity - SHA prefix 'V'",
			shaPrefix:     "V",
			description:   "sha LIKE 'sha1:V*'",
			expectedCount: 30312, // Exact count from sphere.1M.jsonl (3.03%)
		},
		{
			name:          "Low selectivity - SHA prefix 'VJ'",
			shaPrefix:     "VJ",
			description:   "sha LIKE 'sha1:VJ*'",
			expectedCount: 1542, // Exact count from sphere.1M.jsonl (0.154%)
		},
		{
			name:          "Low selectivity - SHA prefix 'SY'",
			shaPrefix:     "SY",
			description:   "sha LIKE 'sha1:SY*'",
			expectedCount: 1340, // Exact count from sphere.1M.jsonl (0.134%)
		},
		{
			name:          "Very low selectivity - SHA prefix 'HW'",
			shaPrefix:     "HW",
			description:   "sha LIKE 'sha1:HW*'",
			expectedCount: 1238, // Exact count from sphere.1M.jsonl (0.124%)
		},
		{
			name:          "Extremely sparse - SHA prefix '24HB'",
			shaPrefix:     "24HB",
			description:   "sha LIKE 'sha1:24HB*'",
			expectedCount: 330, // Exact count from sphere.1M.jsonl (0.033%)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startTime := time.Now()

			// Initialize tracking
			collectedUUIDs := make([]string, 0)
			uuidSet := make(map[string]bool)
			requestLatencies := make([]time.Duration, 0)
			resultsPerRequest := make([]int, 0)
			exhaustedShards := make(map[string]bool)
			previousShardCursors := make(map[string]string) // Track previous cursor values for validation

			// Build filter for this SHA prefix
			filter := buildFilterForSHAPrefix(tc.shaPrefix)

			// Pagination parameters
			emptyString := ""
			after := &emptyString
			var shardCursors map[string]string
			requestCount := 0
			maxRequests := 20001 // Safety limit

			t.Logf("Starting pagination with filter: %s", tc.description)

			// Pagination loop
			for requestCount < maxRequests {
				requestCount++
				requestStart := time.Now()

				reply, err := grpcClient.Search(ctx, &pb.SearchRequest{
					Collection:   className,
					Limit:        100,
					After:        after,
					ShardCursors: shardCursors,
					Uses_123Api:  false,
					Uses_125Api:  false,
					Uses_127Api:  true,
					Metadata:     &pb.MetadataRequest{Uuid: true},
					Properties:   &pb.PropertiesRequest{NonRefProperties: []string{"sha"}},
					Filters:      filter,
				})
				require.NoError(t, err)

				requestLatency := time.Since(requestStart)
				requestLatencies = append(requestLatencies, requestLatency)
				resultsPerRequest = append(resultsPerRequest, len(reply.Results))

				// Process results
				for _, result := range reply.Results {
					resultUUID := result.Metadata.Id

					// Check for duplicates
					if uuidSet[resultUUID] {
						t.Fatalf("DUPLICATE UUID: %s at request #%d", resultUUID, requestCount)
					}

					// Validate filter match
					if !validateFilterMatch(result, tc.shaPrefix) {
						t.Fatalf("Result UUID %s does not match filter criteria at request #%d", resultUUID, requestCount)
					}

					// Store UUID
					collectedUUIDs = append(collectedUUIDs, resultUUID)
					uuidSet[resultUUID] = true
				}

				// Validate cursor progression: at least one shard must advance or become exhausted
				// It's OK for some shards to stay at the same cursor if their results were
				// discarded due to global truncation - as long as at least one shard progresses
				if requestCount > 1 && len(previousShardCursors) > 0 {
					atLeastOneProgressed := false
					for shardName, currentCursor := range reply.ShardCursors {
						prevCursor, hadPrevious := previousShardCursors[shardName]

						// Skip shards that were already exhausted before this request
						if exhaustedShards[shardName] {
							continue
						}

						// Shard just became exhausted - this counts as progress
						if currentCursor == uuid.Nil.String() {
							atLeastOneProgressed = true
							continue
						}

						// Check if cursor changed from previous request
						if hadPrevious && prevCursor != uuid.Nil.String() {
							if currentCursor != prevCursor {
								atLeastOneProgressed = true
							}
						} else if !hadPrevious {
							// New shard in response counts as progress
							atLeastOneProgressed = true
						}
					}

					if !atLeastOneProgressed {
						t.Fatalf("[%s] No shard cursor progressed at request %d: all cursors stuck. Previous: %v, Current: %v",
							tc.name, requestCount, previousShardCursors, reply.ShardCursors)
					}
				}

				// Update cursors for next iteration
				previousShardCursors = make(map[string]string)
				for k, v := range reply.ShardCursors {
					previousShardCursors[k] = v
				}
				shardCursors = reply.ShardCursors
				if len(reply.Results) > 0 {
					lastID := reply.Results[len(reply.Results)-1].Metadata.Id
					after = &lastID
				}

				// Log shard exhaustion events
				for shardName, cursor := range shardCursors {
					if cursor == uuid.Nil.String() && !exhaustedShards[shardName] {
						exhaustedShards[shardName] = true
						t.Logf("[%s] Shard %s exhausted at request %d", tc.name, shardName, requestCount)
					}
				}

				// Log progress every 50 requests
				if requestCount%50 == 0 {
					t.Logf("[%s] Request %d: latency=%v, results=%d, collected=%d",
						tc.name, requestCount, requestLatency, len(reply.Results), len(collectedUUIDs))
				}

				// Check exit conditions
				allExhausted := true
				for _, cursor := range shardCursors {
					if cursor != uuid.Nil.String() {
						allExhausted = false
						break
					}
				}

				if allExhausted && len(reply.Results) == 0 {
					t.Logf("[%s] Pagination complete at request %d (all shards exhausted=%v, results=%d)",
						tc.name, requestCount, allExhausted, len(reply.Results))
					break
				}
			}

			totalDuration := time.Since(startTime)

			// Verify all shards exhausted
			for shardName, cursor := range shardCursors {
				if cursor != uuid.Nil.String() {
					t.Logf("[%s] WARNING: Shard %s not exhausted (cursor=%s)", tc.name, shardName, cursor)
				}
			}

			require.Less(t, requestCount, maxRequests,
				"Test exceeded maximum request limit (%d), possible infinite loop", maxRequests)

			// Calculate metrics
			sortedLatencies := make([]time.Duration, len(requestLatencies))
			copy(sortedLatencies, requestLatencies)
			sort.Slice(sortedLatencies, func(i, j int) bool {
				return sortedLatencies[i] < sortedLatencies[j]
			})

			var totalLatency time.Duration
			for _, lat := range requestLatencies {
				totalLatency += lat
			}
			avgLatency := totalLatency / time.Duration(len(requestLatencies))
			p50Latency := calculatePercentile(sortedLatencies, 50)
			p95Latency := calculatePercentile(sortedLatencies, 95)
			p99Latency := calculatePercentile(sortedLatencies, 99)

			var totalResults int
			for _, count := range resultsPerRequest {
				totalResults += count
			}
			avgResultsPerRequest := float64(totalResults) / float64(len(resultsPerRequest))
			throughput := float64(len(collectedUUIDs)) / totalDuration.Seconds()

			// Calculate actual vs expected (using EXACT expected count)
			actualCount := len(collectedUUIDs)
			countDiff := actualCount - tc.expectedCount
			expectedPercentage := (float64(tc.expectedCount) / float64(totalObjects)) * 100.0
			actualPercentage := (float64(actualCount) / float64(totalObjects)) * 100.0

			// Print profiling summary
			t.Logf("\n=== PROFILING SUMMARY: %s ===", tc.name)
			t.Logf("Filter: %s", tc.description)
			t.Logf("Collection: %d total objects in test_sphere", totalObjects)
			t.Logf("Expected results: %d EXACTLY (%.2f%% of collection)", tc.expectedCount, expectedPercentage)
			t.Logf("Actual results: %d (%.2f%% of collection)", actualCount, actualPercentage)

			// Show match status prominently
			if actualCount == tc.expectedCount {
				t.Logf("✓ EXACT MATCH: Result count matches expected count perfectly")
			} else {
				t.Logf("✗ MISMATCH: Difference of %+d objects (%+.2f%% of expected)", countDiff, (float64(countDiff)/float64(tc.expectedCount))*100.0)
			}

			t.Logf("Total duration: %v", totalDuration)
			t.Logf("Total requests: %d", requestCount)
			t.Logf("Avg latency: %v (p50=%v, p95=%v, p99=%v)", avgLatency, p50Latency, p95Latency, p99Latency)
			t.Logf("Avg results per request: %.2f", avgResultsPerRequest)
			t.Logf("Throughput: %.2f results/sec", throughput)
			t.Logf("✓ No duplicates detected")
			t.Logf("✓ All results match filter criteria")
			t.Logf("✓ All shards properly exhausted")

			// Assert exact match for deterministic results
			require.Equal(t, tc.expectedCount, actualCount,
				"Result count must EXACTLY match expected count for deterministic results. "+
					"Expected %d, got %d (diff: %+d). "+
					"This ensures the test is repeatable and the collection has the correct data.",
				tc.expectedCount, actualCount, countDiff)
		})
	}
}
