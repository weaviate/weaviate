//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	grpchelper "github.com/weaviate/weaviate/test/helper/grpc"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func testRerankerTransformers(rest, grpc, ollamaEndpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		helper.SetupGRPCClient(t, grpc)
		booksClass := books.ClassModel2VecVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		assertResults := func(t *testing.T, result *graphqlhelper.GraphQLResult, checkGenerate bool) {
			booksResponse := result.Get("Get", "Books").AsSlice()
			require.True(t, len(booksResponse) > 0)
			results, ok := booksResponse[0].(map[string]any)
			require.True(t, ok)
			assert.True(t, results["title"] != nil)
			assert.NotNil(t, results["_additional"])
			additional, ok := results["_additional"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, books.Dune.String(), additional["id"])
			assert.NotNil(t, additional["rerank"])
			rerank, ok := additional["rerank"].([]any)
			require.True(t, ok)
			score, ok := rerank[0].(map[string]any)
			require.True(t, ok)
			require.NotNil(t, score)
			assert.NotNil(t, score["score"])

			if checkGenerate {
				generateExists := false
				for _, obj := range booksResponse {
					book, ok := obj.(map[string]any)
					require.True(t, ok)
					assert.NotNil(t, book["_additional"])
					additional, ok := book["_additional"].(map[string]any)
					require.True(t, ok)
					generate, ok := additional["generate"].(map[string]any)
					if ok {
						generateExists = true
						if generate["error"] != nil {
							t.Logf("\n----------------\nerror: %v\n----------------\n", generate["error"])
						}
						require.Nil(t, generate["error"])
						debug, ok := generate["debug"].(map[string]any)
						require.True(t, ok)
						prompt, ok := debug["prompt"].(string)
						require.True(t, ok)
						assert.True(t, len(prompt) > 0)
						groupedResult, ok := generate["groupedResult"].(string)
						require.True(t, ok)
						assert.True(t, len(groupedResult) > 0)
						t.Logf("---------------\nDebug.Prompt: %v\n\nGroupedResult: %v\n---------------\n", prompt, groupedResult)
					}
				}
				require.True(t, generateExists)
			}
		}

		t.Run("import data", func(t *testing.T) {
			for _, book := range books.Objects() {
				helper.CreateObject(t, book)
				helper.AssertGetObjectEventually(t, book.Class, book.ID)
			}
		})

		t.Run("rerank", func(t *testing.T) {
			query := `
			{
				Get {
					Books
					%s
					{
						title
						_additional{
							id
							rerank(property:"description", query: "Who is the author of Dune?") {
								score
							}
						}
					}
				}
			}`
			tests := []struct {
				name           string
				searchArgument string
			}{
				{
					name: "without search argument",
				},
				{
					name:           "with nearText",
					searchArgument: `(nearText:{concepts:"project hail mary"})`,
				},
				{
					name:           "with nearObject",
					searchArgument: fmt.Sprintf("(nearObject:{id:%q})", books.TheLordOfTheIceGarden),
				},
				{
					name:           "with hybrid",
					searchArgument: `(hybrid:{query:"project hail mary" properties:["title"]})`,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					assertResults(t, graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.searchArgument)), false)
				})
			}
		})

		t.Run("rerank with generate", func(t *testing.T) {
			query := `
			{
				Get {
					Books
					%s
					{
						title
						_additional{
							id
							rerank(property:"description", query: "Who is the author of Dune?") {
								score
							}
							generate(
								groupedResult:{
									ollama:{
										apiEndpoint:%q
										model:"tinyllama"
									}
									task:"Write a short tweet about this content"
									properties:["title","description"]
									debug:true
								}
							){
								groupedResult
								error
								debug{
									prompt
								}
							}
						}
					}
				}
			}`
			tests := []struct {
				name           string
				searchArgument string
			}{
				{
					name: "without search argument",
				},
				{
					name:           "with nearText",
					searchArgument: `(nearText:{concepts:"project hail mary"})`,
				},
				{
					name:           "with nearObject",
					searchArgument: fmt.Sprintf("(nearObject:{id:%q})", books.TheLordOfTheIceGarden),
				},
				{
					name:           "with hybrid",
					searchArgument: `(hybrid:{query:"project hail mary" properties:["title"]})`,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.searchArgument, ollamaEndpoint))
					assertResults(t, result, true)
				})
			}
		})

		t.Run("rerank using gRPC", func(t *testing.T) {
			tests := []struct {
				name         string
				nearText     *pb.NearTextSearch
				nearObject   *pb.NearObject
				hybrid       *pb.Hybrid
				withGenerate bool
			}{
				{
					name: "without search argument",
				},
				{
					name:         "without search argument with generate",
					withGenerate: true,
				},
				{
					name: "with nearText",
					nearText: &pb.NearTextSearch{
						Query: []string{"project hail mary"},
					},
				},
				{
					name: "with nearText with generate",
					nearText: &pb.NearTextSearch{
						Query: []string{"project hail mary"},
					},
					withGenerate: true,
				},
				{
					name: "with nearObject",
					nearObject: &pb.NearObject{
						Id: books.TheLordOfTheIceGarden.String(),
					},
				},
				{
					name: "with nearObject with generate",
					nearObject: &pb.NearObject{
						Id: books.TheLordOfTheIceGarden.String(),
					},
					withGenerate: true,
				},
				{
					name: "with hybrid",
					hybrid: &pb.Hybrid{
						Query:      "project hail mary",
						Alpha:      0.5,
						Properties: []string{"title", "description"},
					},
				},
				{
					name: "with hybrid with generate",
					hybrid: &pb.Hybrid{
						Query:      "project hail mary",
						Alpha:      0.5,
						Properties: []string{"title", "description"},
					},
					withGenerate: true,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					req := &pb.SearchRequest{
						Collection:   booksClass.Class,
						NearText:     tt.nearText,
						NearObject:   tt.nearObject,
						HybridSearch: tt.hybrid,
						Rerank: &pb.Rerank{
							Property: "description",
							Query:    grpchelper.ToPtr("Who is the author of Dune?"),
						},
						Metadata: &pb.MetadataRequest{
							Score: true,
						},
						Uses_127Api: true,
					}
					if tt.withGenerate {
						req.Generative = &pb.GenerativeSearch{
							Grouped: &pb.GenerativeSearch_Grouped{
								Task:       "Write a short tweet about this content",
								Debug:      true,
								Properties: &pb.TextArray{Values: []string{"title", "description"}},
								Queries: []*pb.GenerativeProvider{{
									ReturnMetadata: false, // no metadata for ollama
									Kind: &pb.GenerativeProvider_Ollama{
										Ollama: &pb.GenerativeOllama{
											ApiEndpoint: grpchelper.ToPtr(ollamaEndpoint),
											Model:       grpchelper.ToPtr("tinyllama"),
										},
									},
								}},
							},
						}
					}
					resp := grpchelper.AssertSearchWithTimeout(t, req, 10*time.Minute)
					require.NotNil(t, resp)
					require.NotEmpty(t, resp.Results)
					require.Len(t, resp.Results, 3)
					require.NotNil(t, resp.Results[0].Metadata)
					require.True(t, resp.Results[0].Metadata.RerankScorePresent)
					assert.GreaterOrEqual(t, resp.Results[0].Metadata.RerankScore, float64(8))
					if tt.withGenerate {
						require.NotNil(t, resp.GenerativeGroupedResults)
						require.NotEmpty(t, resp.GenerativeGroupedResults.Values)
						require.NotEmpty(t, resp.GenerativeGroupedResults.Values[0].Result)
						require.NotNil(t, resp.GenerativeGroupedResults.Values[0].Debug)
						require.NotEmpty(t, *resp.GenerativeGroupedResults.Values[0].Debug.FullPrompt)
						t.Logf("---------------\nDebug.Prompt: %v\n\nGroupedResult: %v\n---------------\n", *resp.GenerativeGroupedResults.Values[0].Debug.FullPrompt, resp.GenerativeGroupedResults.Values[0].Result)
					}
				})
			}
		})
	}
}
