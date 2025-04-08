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

package generative

import (
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	anthropicClients "github.com/weaviate/weaviate/modules/generative-anthropic/clients"
	anthropicParams "github.com/weaviate/weaviate/modules/generative-anthropic/parameters"
	anyscaleParams "github.com/weaviate/weaviate/modules/generative-anyscale/parameters"
	awsParams "github.com/weaviate/weaviate/modules/generative-aws/parameters"
	cohereClients "github.com/weaviate/weaviate/modules/generative-cohere/clients"
	cohereParams "github.com/weaviate/weaviate/modules/generative-cohere/parameters"
	databricksClients "github.com/weaviate/weaviate/modules/generative-databricks/clients"
	databricksParams "github.com/weaviate/weaviate/modules/generative-databricks/parameters"
	friendliClients "github.com/weaviate/weaviate/modules/generative-friendliai/clients"
	friendliParams "github.com/weaviate/weaviate/modules/generative-friendliai/parameters"
	googleClients "github.com/weaviate/weaviate/modules/generative-google/clients"
	googleParams "github.com/weaviate/weaviate/modules/generative-google/parameters"
	mistralClients "github.com/weaviate/weaviate/modules/generative-mistral/clients"
	mistralParams "github.com/weaviate/weaviate/modules/generative-mistral/parameters"
	nvidiaClients "github.com/weaviate/weaviate/modules/generative-nvidia/clients"
	nvidiaParams "github.com/weaviate/weaviate/modules/generative-nvidia/parameters"
	ollamaParams "github.com/weaviate/weaviate/modules/generative-ollama/parameters"
	openaiClients "github.com/weaviate/weaviate/modules/generative-openai/clients"
	openaiParams "github.com/weaviate/weaviate/modules/generative-openai/parameters"
	xaiClients "github.com/weaviate/weaviate/modules/generative-xai/clients"
	xaiParams "github.com/weaviate/weaviate/modules/generative-xai/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
	additionalModels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

type Replier struct {
	logger      logrus.FieldLogger
	queryParams queryParams
	uses127Api  bool
}

type queryParams interface {
	ProviderName() string
	ReturnMetadataForSingle() bool
	ReturnMetadataForGrouped() bool
	Debug() bool
}

func NewReplier(logger logrus.FieldLogger, queryParams queryParams, uses127Api bool) *Replier {
	return &Replier{
		logger:      logger,
		queryParams: queryParams,
		uses127Api:  uses127Api,
	}
}

func (r *Replier) Extract(_additional map[string]any, params any, metadata *pb.MetadataResult) (*pb.GenerativeResult, *pb.GenerativeResult, string, error) {
	if r.uses127Api {
		single, grouped, err := r.extractGenerativeResult(_additional, params)
		return single, grouped, "", err
	} else {
		grouped, err := r.extractDeprecated(_additional, params, metadata)
		if err != nil {
			return nil, nil, "", err
		}
		return nil, nil, grouped, nil
	}
}

func (r *Replier) extractGenerativeResult(_additional map[string]any, params any) (*pb.GenerativeResult, *pb.GenerativeResult, error) {
	single, grouped, err := r.extractGenerativeReply(_additional, params)
	if err != nil {
		return nil, nil, err
	}
	return &pb.GenerativeResult{Values: []*pb.GenerativeReply{single}}, &pb.GenerativeResult{Values: []*pb.GenerativeReply{grouped}}, nil
}

func (r *Replier) extractDeprecated(_additional map[string]any, params any, metadata *pb.MetadataResult) (string, error) {
	var generativeGroupResults string
	generateFmt, err := r.extractGenerateResultDeprecated(_additional, params)
	if err != nil {
		return "", err
	}

	if generateFmt.SingleResult != nil && *generateFmt.SingleResult != "" {
		metadata.Generative = *generateFmt.SingleResult
		metadata.GenerativePresent = true
	}

	// grouped results are only added to the first object for GQL reasons
	// however, reranking can result in a different order, so we need to check every object
	// recording the result if it's present assuming that it is at least somewhere and will be caught
	if generateFmt.GroupedResult != nil && *generateFmt.GroupedResult != "" {
		generativeGroupResults = *generateFmt.GroupedResult
	}
	return generativeGroupResults, nil
}

func (r *Replier) extractGenerateResultDeprecated(_additional map[string]any, params any) (*additionalModels.GenerateResult, error) {
	generateFmt := &additionalModels.GenerateResult{}
	if generate, ok := _additional["generate"]; ok {
		generateParams, ok := generate.(map[string]any)
		if !ok {
			return nil, errors.New("could not cast generative result additional prop")
		}
		if generateParams["singleResult"] != nil {
			if singleResult, ok := generateParams["singleResult"].(*string); ok {
				generateFmt.SingleResult = singleResult
			}
		}
		if generateParams["groupedResult"] != nil {
			if groupedResult, ok := generateParams["groupedResult"].(*string); ok {
				generateFmt.GroupedResult = groupedResult
			}
		}
		if generateParams["error"] != nil {
			if err, ok := generateParams["error"].(error); ok {
				generateFmt.Error = err
			}
		}
	}
	if generateFmt.Error != nil {
		return nil, generateFmt.Error
	}
	generativeSearch, ok := params.(*generate.Params)
	if !ok {
		return nil, errors.New("could not cast generative search params")
	}
	if generativeSearch.Prompt != nil && generateFmt.SingleResult == nil {
		return nil, errors.New("no results for generative search despite a search request. Is a generative module enabled?")
	}
	return generateFmt, nil
}

func (r *Replier) extractGenerativeMetadata(results map[string]any) (*pb.GenerativeMetadata, error) {
	metadata := &pb.GenerativeMetadata{}
	providerName := r.queryParams.ProviderName()
	switch providerName {
	case anthropicParams.Name:
		params := anthropicClients.GetResponseParams(results)
		if params == nil {
			r.logger.WithField("results", results).WithField("provider", providerName).Error("could not get metadata")
			return metadata, nil
		}
		anthropic := &pb.GenerativeAnthropicMetadata{}
		if params.Usage != nil {
			anthropic.Usage = &pb.GenerativeAnthropicMetadata_Usage{
				InputTokens:  int64(params.Usage.InputTokens),
				OutputTokens: int64(params.Usage.OutputTokens),
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Anthropic{Anthropic: anthropic}
	case anyscaleParams.Name:
		// Do nothing, no metadata for Anyscale
	case awsParams.Name:
		// Do nothing, no metadata for AWS
	case cohereParams.Name:
		params := cohereClients.GetResponseParams(results)
		if params == nil {
			return nil, fmt.Errorf("could not get request metadata for provider: %s", providerName)
		}
		cohere := &pb.GenerativeCohereMetadata{}
		if params.Meta != nil {
			if params.Meta.ApiVersion != nil {
				cohere.ApiVersion = &pb.GenerativeCohereMetadata_ApiVersion{
					Version:        params.Meta.ApiVersion.Version,
					IsDeprecated:   params.Meta.ApiVersion.IsDeprecated,
					IsExperimental: params.Meta.ApiVersion.IsExperimental,
				}
			}
			if params.Meta.BilledUnits != nil {
				cohere.BilledUnits = &pb.GenerativeCohereMetadata_BilledUnits{
					InputTokens:     params.Meta.BilledUnits.InputTokens,
					OutputTokens:    params.Meta.BilledUnits.OutputTokens,
					SearchUnits:     params.Meta.BilledUnits.SearchUnits,
					Classifications: params.Meta.BilledUnits.Classifications,
				}
			}
			if params.Meta.Tokens != nil {
				cohere.Tokens = &pb.GenerativeCohereMetadata_Tokens{
					InputTokens:  params.Meta.Tokens.InputTokens,
					OutputTokens: params.Meta.Tokens.OutputTokens,
				}
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Cohere{Cohere: cohere}
	case mistralParams.Name:
		params := mistralClients.GetResponseParams(results)
		if params == nil {
			return nil, fmt.Errorf("could not get request metadata for provider: %s from results: %+v", providerName, results)
		}
		mistral := &pb.GenerativeMistralMetadata{}
		if params.Usage != nil {
			mistral.Usage = &pb.GenerativeMistralMetadata_Usage{
				PromptTokens:     convertIntPtrToInt64Ptr(params.Usage.PromptTokens),
				CompletionTokens: convertIntPtrToInt64Ptr(params.Usage.CompletionTokens),
				TotalTokens:      convertIntPtrToInt64Ptr(params.Usage.TotalTokens),
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Mistral{Mistral: mistral}
	case ollamaParams.Name:
		// Do nothing, no metadata for Ollama
	case openaiParams.Name:
		params := openaiClients.GetResponseParams(results)
		if params == nil {
			return nil, fmt.Errorf("could not get request metadata for provider: %s", providerName)
		}
		openai := &pb.GenerativeOpenAIMetadata{}
		if params.Usage != nil {
			openai.Usage = &pb.GenerativeOpenAIMetadata_Usage{
				PromptTokens:     convertIntPtrToInt64Ptr(params.Usage.PromptTokens),
				CompletionTokens: convertIntPtrToInt64Ptr(params.Usage.CompletionTokens),
				TotalTokens:      convertIntPtrToInt64Ptr(params.Usage.TotalTokens),
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Openai{Openai: openai}
	case googleParams.Name:
		params := googleClients.GetResponseParams(results)
		if params == nil {
			return nil, fmt.Errorf("could not get request metadata for provider: %s", providerName)
		}
		google := &pb.GenerativeGoogleMetadata{}
		if params.Metadata != nil {
			metadata := &pb.GenerativeGoogleMetadata_Metadata{}
			if params.Metadata.TokenMetadata != nil {
				tokenMetadata := &pb.GenerativeGoogleMetadata_TokenMetadata{}
				if params.Metadata.TokenMetadata.InputTokenCount != nil {
					tokenMetadata.InputTokenCount = &pb.GenerativeGoogleMetadata_TokenCount{
						TotalBillableCharacters: &params.Metadata.TokenMetadata.InputTokenCount.TotalBillableCharacters,
						TotalTokens:             &params.Metadata.TokenMetadata.InputTokenCount.TotalTokens,
					}
				}
				if params.Metadata.TokenMetadata.OutputTokenCount != nil {
					tokenMetadata.OutputTokenCount = &pb.GenerativeGoogleMetadata_TokenCount{
						TotalBillableCharacters: &params.Metadata.TokenMetadata.OutputTokenCount.TotalBillableCharacters,
						TotalTokens:             &params.Metadata.TokenMetadata.OutputTokenCount.TotalTokens,
					}
				}
				metadata.TokenMetadata = tokenMetadata
			}
			google.Metadata = metadata
		}
		if params.UsageMetadata != nil {
			google.UsageMetadata = &pb.GenerativeGoogleMetadata_UsageMetadata{
				PromptTokenCount:     convertIntToInt64Ptr(params.UsageMetadata.PromptTokenCount),
				CandidatesTokenCount: convertIntToInt64Ptr(params.UsageMetadata.CandidatesTokenCount),
				TotalTokenCount:      convertIntToInt64Ptr(params.UsageMetadata.TotalTokenCount),
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Google{Google: google}
	case databricksParams.Name:
		params := databricksClients.GetResponseParams(results)
		if params == nil {
			return nil, fmt.Errorf("could not get request metadata for provider: %s", providerName)
		}
		databricks := &pb.GenerativeDatabricksMetadata{}
		if params.Usage != nil {
			databricks.Usage = &pb.GenerativeDatabricksMetadata_Usage{
				PromptTokens:     convertIntPtrToInt64Ptr(params.Usage.PromptTokens),
				CompletionTokens: convertIntPtrToInt64Ptr(params.Usage.CompletionTokens),
				TotalTokens:      convertIntPtrToInt64Ptr(params.Usage.TotalTokens),
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Databricks{Databricks: databricks}
	case friendliParams.Name:
		params := friendliClients.GetResponseParams(results)
		if params == nil {
			return nil, fmt.Errorf("could not get request metadata for provider: %s", providerName)
		}
		friendliai := &pb.GenerativeFriendliAIMetadata{}
		if params.Usage != nil {
			friendliai.Usage = &pb.GenerativeFriendliAIMetadata_Usage{
				PromptTokens:     convertIntPtrToInt64Ptr(params.Usage.PromptTokens),
				CompletionTokens: convertIntPtrToInt64Ptr(params.Usage.CompletionTokens),
				TotalTokens:      convertIntPtrToInt64Ptr(params.Usage.TotalTokens),
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Friendliai{Friendliai: friendliai}
	case nvidiaParams.Name:
		params := nvidiaClients.GetResponseParams(results)
		if params == nil {
			return nil, fmt.Errorf("could not get request metadata for provider: %s", providerName)
		}
		nvidia := &pb.GenerativeNvidiaMetadata{}
		if params.Usage != nil {
			nvidia.Usage = &pb.GenerativeNvidiaMetadata_Usage{
				PromptTokens:     convertIntPtrToInt64Ptr(params.Usage.PromptTokens),
				CompletionTokens: convertIntPtrToInt64Ptr(params.Usage.CompletionTokens),
				TotalTokens:      convertIntPtrToInt64Ptr(params.Usage.TotalTokens),
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Nvidia{Nvidia: nvidia}
	case xaiParams.Name:
		params := xaiClients.GetResponseParams(results)
		if params == nil {
			return nil, fmt.Errorf("could not get request metadata for provider: %s", providerName)
		}
		xai := &pb.GenerativeXAIMetadata{}
		if params.Usage != nil {
			xai.Usage = &pb.GenerativeXAIMetadata_Usage{
				PromptTokens:     convertIntPtrToInt64Ptr(params.Usage.PromptTokens),
				CompletionTokens: convertIntPtrToInt64Ptr(params.Usage.CompletionTokens),
				TotalTokens:      convertIntPtrToInt64Ptr(params.Usage.TotalTokens),
			}
		}
		metadata.Kind = &pb.GenerativeMetadata_Xai{Xai: xai}
	default:
		return nil, fmt.Errorf("provider: %s, not supported", providerName)
	}
	return metadata, nil
}

func (r *Replier) extractGenerativeReply(_additional map[string]any, params any) (*pb.GenerativeReply, *pb.GenerativeReply, error) {
	reply := &pb.GenerativeReply{}
	grouped := &pb.GenerativeReply{}

	generateParams, ok := params.(*generate.Params)
	if !ok {
		return nil, nil, errors.New("could not cast generative search params")
	}

	if generate, ok := _additional["generate"]; ok {
		generateResults, ok := generate.(map[string]any)
		if !ok {
			return nil, nil, errors.New("could not cast generative result additional prop")
		}
		if generateResults["singleResult"] != nil {
			if singleResult, ok := generateResults["singleResult"].(*string); ok && singleResult != nil {
				reply.Result = *singleResult
			}
		} else {
			if generateParams.Prompt != nil {
				return nil, nil, errors.New("no results for generative search despite a search request. Is a generative module enabled?")
			}
		}
		// grouped results are only added to the first object for GQL reasons
		// however, reranking can result in a different order, so we need to check every object
		// recording the result if it's present assuming that it is at least somewhere and will be caught
		if generateResults["groupedResult"] != nil {
			if groupedResult, ok := generateResults["groupedResult"].(*string); ok && groupedResult != nil {
				grouped.Result = *groupedResult
			}
		}
		if generateResults["error"] != nil {
			if err, ok := generateResults["error"].(error); ok {
				return nil, nil, err
			}
		}
		if generateResults["debug"] != nil && r.queryParams.Debug() {
			if debug, ok := generateResults["debug"].(*modulecapabilities.GenerateDebugInformation); ok && debug != nil {
				prompt := debug.Prompt
				reply.Debug = &pb.GenerativeDebug{FullPrompt: &prompt}
			}
		}
		if r.queryParams.ReturnMetadataForSingle() || r.queryParams.ReturnMetadataForGrouped() {
			metadata, err := r.extractGenerativeMetadata(generateResults)
			if err != nil {
				return nil, nil, err
			}
			if r.queryParams.ReturnMetadataForSingle() {
				reply.Metadata = metadata
			}
			g := r.queryParams.ReturnMetadataForGrouped()
			if generateResults["groupedResult"] != nil && g {
				grouped.Metadata = metadata
			}
		}
	}
	return reply, grouped, nil
}

func convertIntPtrToInt64Ptr(i *int) *int64 {
	if i == nil {
		return nil
	}
	converted := int64(*i)
	return &converted
}

func convertIntToInt64Ptr(i int) *int64 {
	converted := int64(i)
	return &converted
}
