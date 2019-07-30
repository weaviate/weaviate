//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package contextionary

import (
	"context"
	"fmt"

	pb "github.com/semi-technologies/contextionary/contextionary"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"google.golang.org/grpc"
)

// Client establishes a gRPC connection to a remote contextionary service
type Client struct {
	grpcClient pb.ContextionaryClient
}

// NewClient from gRPC discovery url to connect to a remote contextionary service
func NewClient(uri string) (*Client, error) {
	conn, err := grpc.Dial(uri, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to remote contextionary gRPC server: %s", err)
	}

	client := pb.NewContextionaryClient(conn)
	return &Client{
		grpcClient: client,
	}, nil
}

// IsStopWord returns true if the given word is a stopword, errors on connection errors
func (c *Client) IsStopWord(ctx context.Context, word string) (bool, error) {
	res, err := c.grpcClient.IsWordStopword(ctx, &pb.Word{Word: word})
	if err != nil {
		return false, err
	}

	return res.Stopword, nil
}

// IsWordPresent returns true if the given word is a stopword, errors on connection errors
func (c *Client) IsWordPresent(ctx context.Context, word string) (bool, error) {
	res, err := c.grpcClient.IsWordPresent(ctx, &pb.Word{Word: word})
	if err != nil {
		return false, err
	}

	return res.Present, nil
}

//SafeGetSimilarWordsWithCertainty will alwasy return a list words - unless there is a network error
func (c *Client) SafeGetSimilarWordsWithCertainty(ctx context.Context, word string, certainty float32) ([]string, error) {
	res, err := c.grpcClient.SafeGetSimilarWordsWithCertainty(ctx, &pb.SimilarWordsParams{Word: word, Certainty: certainty})
	if err != nil {
		return nil, err
	}

	output := make([]string, len(res.Words), len(res.Words))
	for i, word := range res.Words {
		output[i] = word.Word
	}

	return output, nil
}

// SchemaSearch for related classes and properties
func (c *Client) SchemaSearch(ctx context.Context, params traverser.SearchParams) (traverser.SearchResults, error) {
	pbParams := &pb.SchemaSearchParams{
		Certainty:  params.Certainty,
		Name:       params.Name,
		Kind:       kindToProto(params.Kind),
		Keywords:   keywordsToProto(params.Keywords),
		SearchType: searchTypeToProto(params.SearchType),
	}

	res, err := c.grpcClient.SchemaSearch(ctx, pbParams)
	if err != nil {
		return traverser.SearchResults{}, err
	}

	return schemaSearchResultsFromProto(res), nil
}

func kindToProto(k kind.Kind) pb.Kind {
	switch k {
	case kind.Thing:
		return pb.Kind_THING
	case kind.Action:
		return pb.Kind_ACTION
	default:
		panic(fmt.Sprintf("unknown kind %v", k))
	}
}

func kindFromProto(k pb.Kind) kind.Kind {
	switch k {
	case pb.Kind_THING:
		return kind.Thing
	case pb.Kind_ACTION:
		return kind.Action
	default:
		panic(fmt.Sprintf("unknown kind %v", k))
	}
}

func keywordsToProto(kws models.Keywords) []*pb.Keyword {

	output := make([]*pb.Keyword, len(kws), len(kws))
	for i, kw := range kws {
		output[i] = &pb.Keyword{
			Keyword: kw.Keyword,
			Weight:  kw.Weight,
		}
	}

	return output
}

func searchTypeToProto(input traverser.SearchType) pb.SearchType {
	switch input {
	case traverser.SearchTypeClass:
		return pb.SearchType_CLASS
	case traverser.SearchTypeProperty:
		return pb.SearchType_PROPERTY
	default:
		panic(fmt.Sprintf("unknown search type %v", input))
	}
}

func searchTypeFromProto(input pb.SearchType) traverser.SearchType {
	switch input {
	case pb.SearchType_CLASS:
		return traverser.SearchTypeClass
	case pb.SearchType_PROPERTY:
		return traverser.SearchTypeProperty
	default:
		panic(fmt.Sprintf("unknown search type %v", input))
	}
}

func schemaSearchResultsFromProto(res *pb.SchemaSearchResults) traverser.SearchResults {
	return traverser.SearchResults{
		Type:    searchTypeFromProto(res.Type),
		Results: searchResultsFromProto(res.Results),
	}
}

func searchResultsFromProto(input []*pb.SchemaSearchResult) []traverser.SearchResult {
	output := make([]traverser.SearchResult, len(input), len(input))
	for i, res := range input {
		output[i] = traverser.SearchResult{
			Certainty: res.Certainty,
			Name:      res.Name,
			Kind:      kindFromProto(res.Kind),
		}
	}

	return output
}

func (c *Client) VectorForWord(ctx context.Context, word string) ([]float32, error) {
	res, err := c.grpcClient.VectorForWord(ctx, &pb.Word{Word: word})
	if err != nil {
		return nil, fmt.Errorf("could not get vector from remote: %v", err)
	}
	return vectorFromProto(res.Entries), nil
}

func vectorFromProto(in []*pb.VectorEntry) []float32 {
	output := make([]float32, len(in), len(in))
	for i, entry := range in {
		output[i] = entry.Entry
	}

	return output
}

func (c *Client) VectorForCorpi(ctx context.Context, corpi []string) ([]float32, error) {
	res, err := c.grpcClient.VectorForCorpi(ctx, &pb.Corpi{Corpi: corpi})
	if err != nil {
		return nil, fmt.Errorf("could not get vector from remote: %v", err)
	}
	return vectorFromProto(res.Entries), nil
}

func (c *Client) NearestWordsByVector(ctx context.Context, vector []float32, n int, k int) ([]string, []float32, error) {
	res, err := c.grpcClient.NearestWordsByVector(ctx, &pb.VectorNNParams{
		K:      int32(k),
		N:      int32(n),
		Vector: vectorToProto(vector),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("could not get nearest words by vector: %v", err)
	}

	return res.Words, res.Distances, nil
}

func vectorToProto(in []float32) *pb.Vector {
	output := make([]*pb.VectorEntry, len(in), len(in))
	for i, entry := range in {
		output[i] = &pb.VectorEntry{
			Entry: entry,
		}
	}

	return &pb.Vector{Entries: output}
}
