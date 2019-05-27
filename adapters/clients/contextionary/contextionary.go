package contextionary

import (
	"context"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	pb "github.com/semi-technologies/contextionary/contextionary"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
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
func (c *Client) SchemaSearch(ctx context.Context, params kinds.SearchParams) (kinds.SearchResults, error) {
	pbParams := &pb.SchemaSearchParams{
		Certainty:  params.Certainty,
		Name:       params.Name,
		Kind:       kindToProto(params.Kind),
		Keywords:   keywordsToProto(params.Keywords),
		SearchType: searchTypeToProto(params.SearchType),
	}

	fmt.Printf("\n\n\ninput:\n")
	spew.Dump(pbParams)

	res, err := c.grpcClient.SchemaSearch(ctx, pbParams)
	if err != nil {
		return kinds.SearchResults{}, err
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

func keywordsToProto(kws models.SemanticSchemaKeywords) []*pb.Keyword {

	output := make([]*pb.Keyword, len(kws), len(kws))
	for i, kw := range kws {
		output[i] = &pb.Keyword{
			Keyword: kw.Keyword,
			Weight:  kw.Weight,
		}
	}

	return output
}

func searchTypeToProto(input kinds.SearchType) pb.SearchType {
	switch input {
	case kinds.SearchTypeClass:
		return pb.SearchType_CLASS
	case kinds.SearchTypeProperty:
		return pb.SearchType_PROPERTY
	default:
		panic(fmt.Sprintf("unknown search type %v", input))
	}
}

func searchTypeFromProto(input pb.SearchType) kinds.SearchType {
	switch input {
	case pb.SearchType_CLASS:
		return kinds.SearchTypeClass
	case pb.SearchType_PROPERTY:
		return kinds.SearchTypeProperty
	default:
		panic(fmt.Sprintf("unknown search type %v", input))
	}
}

func schemaSearchResultsFromProto(res *pb.SchemaSearchResults) kinds.SearchResults {
	return kinds.SearchResults{
		Type:    searchTypeFromProto(res.Type),
		Results: searchResultsFromProto(res.Results),
	}
}

func searchResultsFromProto(input []*pb.SchemaSearchResult) []kinds.SearchResult {
	output := make([]kinds.SearchResult, len(input), len(input))
	for i, res := range input {
		output[i] = kinds.SearchResult{
			Certainty: res.Certainty,
			Name:      res.Name,
			Kind:      kindFromProto(res.Kind),
		}
	}

	fmt.Printf("\n\n\noutput:\n")
	spew.Dump(output)
	return output
}
