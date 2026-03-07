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

// Command profile_demo demonstrates the query profiling feature against a local Weaviate.
//
// Prerequisites:
//   - Weaviate running locally (e.g. `make local`) with REST on :8080 and gRPC on :50051
//
// Usage:
//
//	go run ./tools/dev/profile_demo
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"time"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	restAddr = "http://localhost:8080"
	grpcAddr = "localhost:50051"
	class    = "ProfileDemo"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	setupCollection(ctx)
	// defer cleanupCollection()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewWeaviateClient(conn)

	vecBytes := float32sToBytes([]float32{0.5, 0.3, 0.2})
	searchVec := &pb.NearVector{
		Vectors: []*pb.Vectors{{
			VectorBytes: vecBytes,
			Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
		}},
	}

	// 1. Vector search (near_vector)
	fmt.Println("=== 1. Vector search with profile ===")
	reply, err := client.Search(ctx, &pb.SearchRequest{
		Collection:  class,
		Limit:       5,
		Metadata:    &pb.MetadataRequest{Uuid: true, Distance: true, Profile: true},
		NearVector:  searchVec,
		Uses_127Api: true,
	})
	if err != nil {
		log.Fatalf("vector search: %v", err)
	}
	fmt.Printf("Results: %d\n", len(reply.Results))
	printProfile(reply.Profile)

	// 2. Vector search + filter
	fmt.Println("\n=== 2. Vector search + filter with profile ===")
	reply, err = client.Search(ctx, &pb.SearchRequest{
		Collection:  class,
		Limit:       5,
		Metadata:    &pb.MetadataRequest{Uuid: true, Distance: true, Profile: true},
		NearVector:  searchVec,
		Uses_127Api: true,
		Filters: &pb.Filters{
			Operator:  pb.Filters_OPERATOR_GREATER_THAN,
			TestValue: &pb.Filters_ValueInt{ValueInt: 10},
			On:        []string{"num"},
		},
	})
	if err != nil {
		log.Fatalf("filtered vector search: %v", err)
	}
	fmt.Printf("Results: %d\n", len(reply.Results))
	printProfile(reply.Profile)

	// 3. BM25 keyword search
	fmt.Println("\n=== 3. BM25 search with profile ===")
	reply, err = client.Search(ctx, &pb.SearchRequest{
		Collection: class,
		Limit:      5,
		Metadata:   &pb.MetadataRequest{Uuid: true, Score: true, Profile: true},
		Bm25Search: &pb.BM25{
			Query:      "hello world document",
			Properties: []string{"text"},
		},
		Uses_127Api: true,
	})
	if err != nil {
		log.Fatalf("bm25 search: %v", err)
	}
	fmt.Printf("Results: %d\n", len(reply.Results))
	printProfile(reply.Profile)

	// 4. Hybrid search
	fmt.Println("\n=== 4. Hybrid search with profile ===")
	reply, err = client.Search(ctx, &pb.SearchRequest{
		Collection: class,
		Limit:      5,
		Metadata:   &pb.MetadataRequest{Uuid: true, Score: true, Profile: true},
		HybridSearch: &pb.Hybrid{
			Query: "hello world document",
			Alpha: 0.5,
			Properties: []string{"text"},
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{{
					VectorBytes: vecBytes,
					Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				}},
			},
		},
		Uses_127Api: true,
	})
	if err != nil {
		log.Fatalf("hybrid search: %v", err)
	}
	fmt.Printf("Results: %d\n", len(reply.Results))
	printProfile(reply.Profile)

	// 5. Hybrid search (keyword-only, alpha=0)
	fmt.Println("\n=== 5. Hybrid search keyword-only (alpha=0) with profile ===")
	reply, err = client.Search(ctx, &pb.SearchRequest{
		Collection: class,
		Limit:      5,
		Metadata:   &pb.MetadataRequest{Uuid: true, Score: true, Profile: true},
		HybridSearch: &pb.Hybrid{
			Query:      "hello world document",
			Alpha:      0,
			Properties: []string{"text"},
		},
		Uses_127Api: true,
	})
	if err != nil {
		log.Fatalf("hybrid keyword-only search: %v", err)
	}
	fmt.Printf("Results: %d\n", len(reply.Results))
	printProfile(reply.Profile)

	// 6. Hybrid search (vector-only, alpha=1)
	fmt.Println("\n=== 6. Hybrid search vector-only (alpha=1) with profile ===")
	reply, err = client.Search(ctx, &pb.SearchRequest{
		Collection: class,
		Limit:      5,
		Metadata:   &pb.MetadataRequest{Uuid: true, Score: true, Profile: true},
		HybridSearch: &pb.Hybrid{
			Query: "hello world document",
			Alpha: 1,
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{{
					VectorBytes: vecBytes,
					Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				}},
			},
		},
		Uses_127Api: true,
	})
	if err != nil {
		log.Fatalf("hybrid vector-only search: %v", err)
	}
	fmt.Printf("Results: %d\n", len(reply.Results))
	printProfile(reply.Profile)

	// 7. Vector search WITHOUT profile (verify no overhead)
	fmt.Println("\n=== 7. Vector search without profile (baseline) ===")
	reply, err = client.Search(ctx, &pb.SearchRequest{
		Collection:  class,
		Limit:       5,
		Metadata:    &pb.MetadataRequest{Uuid: true, Distance: true},
		NearVector:  searchVec,
		Uses_127Api: true,
	})
	if err != nil {
		log.Fatalf("baseline search: %v", err)
	}
	fmt.Printf("Results: %d, Profile: %v\n", len(reply.Results), reply.Profile)

	// 8. GraphQL vector search with profile
	fmt.Println("\n=== 8. GraphQL vector search with profile ===")
	gqlQuery := map[string]any{
		"query": fmt.Sprintf(`{ Get { %s(nearVector: {vector: [0.5, 0.3, 0.2]}, limit: 3) { text _additional { distance profile } } } }`, class),
	}
	gqlBody, _ := json.Marshal(gqlQuery)
	gqlResp, err := http.Post(restAddr+"/v1/graphql", "application/json", bytes.NewReader(gqlBody))
	if err != nil {
		log.Fatalf("graphql search: %v", err)
	}
	defer gqlResp.Body.Close()
	var gqlResult map[string]any
	json.NewDecoder(gqlResp.Body).Decode(&gqlResult)
	prettyJSON, _ := json.MarshalIndent(gqlResult, "", "  ")
	fmt.Println(string(prettyJSON))

	// 9. GraphQL BM25 search with profile
	fmt.Println("\n=== 9. GraphQL BM25 search with profile ===")
	gqlQuery = map[string]any{
		"query": fmt.Sprintf(`{ Get { %s(bm25: {query: "hello world document", properties: ["text"]}, limit: 3) { text _additional { score profile } } } }`, class),
	}
	gqlBody, _ = json.Marshal(gqlQuery)
	gqlResp, err = http.Post(restAddr+"/v1/graphql", "application/json", bytes.NewReader(gqlBody))
	if err != nil {
		log.Fatalf("graphql bm25: %v", err)
	}
	defer gqlResp.Body.Close()
	json.NewDecoder(gqlResp.Body).Decode(&gqlResult)
	prettyJSON, _ = json.MarshalIndent(gqlResult, "", "  ")
	fmt.Println(string(prettyJSON))

	// 10. GraphQL hybrid search with profile (using nearVector since vectorizer is "none")
	fmt.Println("\n=== 10. GraphQL hybrid search with profile ===")
	gqlQuery = map[string]any{
		"query": fmt.Sprintf(`{ Get { %s(hybrid: {query: "hello world document", alpha: 0.5, properties: ["text"], vector: [0.5, 0.3, 0.2]}, limit: 3) { text _additional { score profile } } } }`, class),
	}
	gqlBody, _ = json.Marshal(gqlQuery)
	gqlResp, err = http.Post(restAddr+"/v1/graphql", "application/json", bytes.NewReader(gqlBody))
	if err != nil {
		log.Fatalf("graphql hybrid: %v", err)
	}
	defer gqlResp.Body.Close()
	json.NewDecoder(gqlResp.Body).Decode(&gqlResult)
	prettyJSON, _ = json.MarshalIndent(gqlResult, "", "  ")
	fmt.Println(string(prettyJSON))
}

func printProfile(profile *pb.QueryProfile) {
	if profile == nil {
		fmt.Println("Profile: <nil>")
		return
	}
	for _, s := range profile.Shards {
		fmt.Printf("\nShard: %s\n", s.Name)
		searchTypes := make([]string, 0, len(s.Searches))
		for st := range s.Searches {
			searchTypes = append(searchTypes, st)
		}
		sort.Strings(searchTypes)
		for _, st := range searchTypes {
			sp := s.Searches[st]
			fmt.Printf("  [%s]\n", st)
			keys := make([]string, 0, len(sp.Details))
			for k := range sp.Details {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("    %-38s %s\n", k, sp.Details[k])
			}
		}
	}
}

func float32sToBytes(v []float32) []byte {
	buf := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

func setupCollection(ctx context.Context) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, restAddr+"/v1/schema/"+class, nil)
	http.DefaultClient.Do(req)

	classBody := map[string]any{
		"class": class,
		"properties": []map[string]any{
			{"name": "text", "dataType": []string{"text"}},
			{"name": "num", "dataType": []string{"int"}},
		},
		"vectorizer": "none",
		"shardingConfig": map[string]any{
			"desiredCount": 3,
		},
	}
	b, _ := json.Marshal(classBody)
	resp, err := http.Post(restAddr+"/v1/schema", "application/json", bytes.NewReader(b))
	if err != nil {
		log.Fatalf("create class: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Fatalf("create class: status %d", resp.StatusCode)
	}

	for i := 1; i <= 100; i++ {
		obj := map[string]any{
			"class": class,
			"properties": map[string]any{
				"text": fmt.Sprintf("hello world document number %d about topic %d", i, i%5),
				"num":  i,
			},
			"vector": []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i)*0.05 + 0.1},
		}
		b, _ := json.Marshal(obj)
		resp, err := http.Post(restAddr+"/v1/objects", "application/json", bytes.NewReader(b))
		if err != nil {
			log.Fatalf("insert object %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Fatalf("insert object %d: status %d", i, resp.StatusCode)
		}
	}
	fmt.Printf("Setup: created collection %q with 100 objects\n", class)
	fmt.Println("Waiting for vector index to build...")
	time.Sleep(2 * time.Second)
	fmt.Println()
}

func cleanupCollection() {
	req, _ := http.NewRequest(http.MethodDelete, restAddr+"/v1/schema/"+class, nil)
	http.DefaultClient.Do(req)
}
