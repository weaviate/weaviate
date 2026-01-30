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

func TestIteratorWithFilterGRPC_MultiShard_NoMissedObjects(t *testing.T) {
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
