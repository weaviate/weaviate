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

package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	pb "github.com/weaviate/contextionary/contextionary"
	"github.com/weaviate/weaviate/entities/models"
	txt2vecmodels "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/vectorizer"
	"github.com/weaviate/weaviate/usecases/traverser"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const ModelUncontactable = "module uncontactable"

// Client establishes a gRPC connection to a remote contextionary service
type Client struct {
	grpcClient pb.ContextionaryClient
	logger     logrus.FieldLogger
}

// NewClient from gRPC discovery url to connect to a remote contextionary service
func NewClient(uri string, logger logrus.FieldLogger) (*Client, error) {
	conn, err := grpc.Dial(uri,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*48)))
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to remote contextionary gRPC server: %s", err)
	}

	client := pb.NewContextionaryClient(conn)
	return &Client{
		grpcClient: client,
		logger:     logger,
	}, nil
}

// IsStopWord returns true if the given word is a stopword, errors on connection errors
func (c *Client) IsStopWord(ctx context.Context, word string) (bool, error) {
	res, err := c.grpcClient.IsWordStopword(ctx, &pb.Word{Word: word})
	if err != nil {
		logConnectionRefused(c.logger, err)
		return false, err
	}

	return res.Stopword, nil
}

// IsWordPresent returns true if the given word is a stopword, errors on connection errors
func (c *Client) IsWordPresent(ctx context.Context, word string) (bool, error) {
	res, err := c.grpcClient.IsWordPresent(ctx, &pb.Word{Word: word})
	if err != nil {
		logConnectionRefused(c.logger, err)
		return false, err
	}

	return res.Present, nil
}

// SafeGetSimilarWordsWithCertainty will always return a list words - unless there is a network error
func (c *Client) SafeGetSimilarWordsWithCertainty(ctx context.Context, word string, certainty float32) ([]string, error) {
	res, err := c.grpcClient.SafeGetSimilarWordsWithCertainty(ctx, &pb.SimilarWordsParams{Word: word, Certainty: certainty})
	if err != nil {
		logConnectionRefused(c.logger, err)
		return nil, err
	}

	output := make([]string, len(res.Words))
	for i, word := range res.Words {
		output[i] = word.Word
	}

	return output, nil
}

// SchemaSearch for related classes and properties
// TODO: is this still used?
func (c *Client) SchemaSearch(ctx context.Context, params traverser.SearchParams) (traverser.SearchResults, error) {
	pbParams := &pb.SchemaSearchParams{
		Certainty:  params.Certainty,
		Name:       params.Name,
		SearchType: searchTypeToProto(params.SearchType),
	}

	res, err := c.grpcClient.SchemaSearch(ctx, pbParams)
	if err != nil {
		logConnectionRefused(c.logger, err)
		return traverser.SearchResults{}, err
	}

	return schemaSearchResultsFromProto(res), nil
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
	output := make([]traverser.SearchResult, len(input))
	for i, res := range input {
		output[i] = traverser.SearchResult{
			Certainty: res.Certainty,
			Name:      res.Name,
		}
	}

	return output
}

func (c *Client) VectorForWord(ctx context.Context, word string) ([]float32, error) {
	res, err := c.grpcClient.VectorForWord(ctx, &pb.Word{Word: word})
	if err != nil {
		logConnectionRefused(c.logger, err)
		return nil, fmt.Errorf("could not get vector from remote: %v", err)
	}
	v, _, _ := vectorFromProto(res)
	return v, nil
}

func logConnectionRefused(logger logrus.FieldLogger, err error) {
	if strings.Contains(fmt.Sprintf("%v", err), "connect: connection refused") {
		logger.WithError(err).WithField("module", "contextionary").Warnf(ModelUncontactable)
	} else if strings.Contains(err.Error(), "connectex: No connection could be made because the target machine actively refused it.") {
		logger.WithError(err).WithField("module", "contextionary").Warnf(ModelUncontactable)
	}
}

func (c *Client) MultiVectorForWord(ctx context.Context, words []string) ([][]float32, error) {
	out := make([][]float32, len(words))
	wordParams := make([]*pb.Word, len(words))

	for i, word := range words {
		wordParams[i] = &pb.Word{Word: word}
	}

	res, err := c.grpcClient.MultiVectorForWord(ctx, &pb.WordList{Words: wordParams})
	if err != nil {
		logConnectionRefused(c.logger, err)
		return nil, err
	}

	for i, elem := range res.Vectors {
		if len(elem.Entries) == 0 {
			// indicates word not found
			continue
		}

		out[i], _, _ = vectorFromProto(elem)
	}

	return out, nil
}

func (c *Client) MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([]*txt2vecmodels.NearestNeighbors, error) {
	out := make([]*txt2vecmodels.NearestNeighbors, len(vectors))
	searchParams := make([]*pb.VectorNNParams, len(vectors))

	for i, vector := range vectors {
		searchParams[i] = &pb.VectorNNParams{
			Vector: vectorToProto(vector),
			K:      int32(k),
			N:      int32(n),
		}
	}

	res, err := c.grpcClient.MultiNearestWordsByVector(ctx, &pb.VectorNNParamsList{Params: searchParams})
	if err != nil {
		logConnectionRefused(c.logger, err)
		return nil, err
	}

	for i, elem := range res.Words {
		out[i] = &txt2vecmodels.NearestNeighbors{
			Neighbors: c.extractNeighbors(elem),
		}
	}

	return out, nil
}

func (c *Client) extractNeighbors(elem *pb.NearestWords) []*txt2vecmodels.NearestNeighbor {
	out := make([]*txt2vecmodels.NearestNeighbor, len(elem.Words))

	for i := range out {
		vec, _, _ := vectorFromProto(elem.Vectors.Vectors[i])
		out[i] = &txt2vecmodels.NearestNeighbor{
			Concept:  elem.Words[i],
			Distance: elem.Distances[i],
			Vector:   vec,
		}
	}
	return out
}

func vectorFromProto(in *pb.Vector) ([]float32, []txt2vecmodels.InterpretationSource, error) {
	output := make([]float32, len(in.Entries))
	for i, entry := range in.Entries {
		output[i] = entry.Entry
	}

	source := make([]txt2vecmodels.InterpretationSource, len(in.Source))
	for i, s := range in.Source {
		source[i].Concept = s.Concept
		source[i].Weight = float64(s.Weight)
		source[i].Occurrence = s.Occurrence
	}

	return output, source, nil
}

func (c *Client) VectorForCorpi(ctx context.Context, corpi []string, overridesMap map[string]string) ([]float32, []txt2vecmodels.InterpretationSource, error) {
	overrides := overridesFromMap(overridesMap)
	res, err := c.grpcClient.VectorForCorpi(ctx, &pb.Corpi{Corpi: corpi, Overrides: overrides})
	if err != nil {
		if strings.Contains(err.Error(), "connect: connection refused") {
			c.logger.WithError(err).WithField("module", "contextionary").Warnf(ModelUncontactable)
		} else if strings.Contains(err.Error(), "connectex: No connection could be made because the target machine actively refused it.") {
			c.logger.WithError(err).WithField("module", "contextionary").Warnf(ModelUncontactable)
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			return nil, nil, fmt.Errorf("could not get vector from remote: %v", err)
		}

		return nil, nil, vectorizer.NewErrNoUsableWordsf(st.Message())
	}

	return vectorFromProto(res)
}

func (c *Client) VectorOnlyForCorpi(ctx context.Context, corpi []string, overrides map[string]string) ([]float32, error) {
	vec, _, err := c.VectorForCorpi(ctx, corpi, overrides)
	return vec, err
}

func (c *Client) NearestWordsByVector(ctx context.Context, vector []float32, n int, k int) ([]string, []float32, error) {
	res, err := c.grpcClient.NearestWordsByVector(ctx, &pb.VectorNNParams{
		K:      int32(k),
		N:      int32(n),
		Vector: vectorToProto(vector),
	})
	if err != nil {
		logConnectionRefused(c.logger, err)
		return nil, nil, fmt.Errorf("could not get nearest words by vector: %v", err)
	}

	return res.Words, res.Distances, nil
}

func (c *Client) AddExtension(ctx context.Context, extension *models.C11yExtension) error {
	_, err := c.grpcClient.AddExtension(ctx, &pb.ExtensionInput{
		Concept:    extension.Concept,
		Definition: strings.ToLower(extension.Definition),
		Weight:     extension.Weight,
	})

	return err
}

func vectorToProto(in []float32) *pb.Vector {
	output := make([]*pb.VectorEntry, len(in))
	for i, entry := range in {
		output[i] = &pb.VectorEntry{
			Entry: entry,
		}
	}

	return &pb.Vector{Entries: output}
}

func (c *Client) WaitForStartupAndValidateVersion(startupCtx context.Context,
	requiredMinimumVersion string, interval time.Duration,
) error {
	for {
		if err := startupCtx.Err(); err != nil {
			return errors.Wrap(err, "wait for contextionary remote inference service")
		}

		time.Sleep(interval)

		ctx, cancel := context.WithTimeout(startupCtx, 2*time.Second)
		defer cancel()
		v, err := c.version(ctx)
		if err != nil {
			c.logger.WithField("action", "startup_check_contextionary").WithError(err).
				Warnf("could not connect to contextionary at startup, trying again in 1 sec")
			continue
		}

		ok, err := extractVersionAndCompare(v, requiredMinimumVersion)
		if err != nil {
			c.logger.WithField("action", "startup_check_contextionary").
				WithField("requiredMinimumContextionaryVersion", requiredMinimumVersion).
				WithField("contextionaryVersion", v).
				WithError(err).
				Warnf("cannot determine if contextionary version is compatible. " +
					"This is fine in development, but probelematic if you see this production")
			return nil
		}

		if ok {
			c.logger.WithField("action", "startup_check_contextionary").
				WithField("requiredMinimumContextionaryVersion", requiredMinimumVersion).
				WithField("contextionaryVersion", v).
				Infof("found a valid contextionary version")
			return nil
		} else {
			return errors.Errorf("insuffcient contextionary version: need at least %s, got %s",
				requiredMinimumVersion, v)
		}
	}
}

func overridesFromMap(in map[string]string) []*pb.Override {
	if in == nil {
		return nil
	}

	out := make([]*pb.Override, len(in))
	i := 0
	for key, value := range in {
		out[i] = &pb.Override{
			Word:       key,
			Expression: value,
		}
		i++
	}

	return out
}
