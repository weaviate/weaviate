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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	anthropicParams "github.com/weaviate/weaviate/modules/generative-anthropic/parameters"
	anyscaleParams "github.com/weaviate/weaviate/modules/generative-anyscale/parameters"
	awsParams "github.com/weaviate/weaviate/modules/generative-aws/parameters"
	cohereParams "github.com/weaviate/weaviate/modules/generative-cohere/parameters"
	databricksParams "github.com/weaviate/weaviate/modules/generative-databricks/parameters"
	friendliaiParams "github.com/weaviate/weaviate/modules/generative-friendliai/parameters"
	googleParams "github.com/weaviate/weaviate/modules/generative-google/parameters"
	mistralParams "github.com/weaviate/weaviate/modules/generative-mistral/parameters"
	nvidiaParams "github.com/weaviate/weaviate/modules/generative-nvidia/parameters"
	ollamaParams "github.com/weaviate/weaviate/modules/generative-ollama/parameters"
	openaiParams "github.com/weaviate/weaviate/modules/generative-openai/parameters"
	xaiParams "github.com/weaviate/weaviate/modules/generative-xai/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
)

type Parser struct {
	uses127Api     bool
	providerName   string
	returnMetadata returnMetadata
	debug          bool
}

type returnMetadata struct {
	single  bool
	grouped bool
}

func NewParser(uses127Api bool) *Parser {
	return &Parser{
		uses127Api:     uses127Api,
		returnMetadata: returnMetadata{},
	}
}

func (p *Parser) Extract(req *pb.GenerativeSearch, class *models.Class) *generate.Params {
	if req == nil {
		return nil
	}
	if p.uses127Api {
		return p.extract(req, class)
	} else {
		return p.extractDeprecated(req, class)
	}
}

func (p *Parser) ProviderName() string {
	return p.providerName
}

func (p *Parser) ReturnMetadataForSingle() bool {
	return p.returnMetadata.single
}

func (p *Parser) ReturnMetadataForGrouped() bool {
	return p.returnMetadata.grouped
}

func (p *Parser) Debug() bool {
	return p.debug
}

func (p *Parser) extractDeprecated(req *pb.GenerativeSearch, class *models.Class) *generate.Params {
	generative := generate.Params{}
	if req.SingleResponsePrompt != "" {
		generative.Prompt = &req.SingleResponsePrompt
		singleResultPrompts := generate.ExtractPropsFromPrompt(generative.Prompt)
		generative.PropertiesToExtract = append(generative.PropertiesToExtract, singleResultPrompts...)
	}
	if req.GroupedResponseTask != "" {
		generative.Task = &req.GroupedResponseTask
		if len(req.GroupedProperties) > 0 {
			generative.Properties = req.GroupedProperties
			generative.PropertiesToExtract = append(generative.PropertiesToExtract, generative.Properties...)
		} else {
			// if users do not supply a properties, all properties need to be extracted
			generative.PropertiesToExtract = append(generative.PropertiesToExtract, schema.GetPropertyNamesFromClass(class, false)...)
		}
	}
	return &generative
}

func (p *Parser) extractFromQuery(generative *generate.Params, queries []*pb.GenerativeProvider) bool {
	if len(queries) == 0 {
		return false
	}
	query := queries[0]
	switch query.Kind.(type) {
	case *pb.GenerativeProvider_Anthropic:
		opts := query.GetAnthropic()
		if opts.GetImageProperties() != nil {
			generative.Properties = append(generative.Properties, opts.GetImageProperties().Values...)
		}
		generative.Options = p.anthropic(opts)
		p.providerName = anthropicParams.Name
	case *pb.GenerativeProvider_Anyscale:
		generative.Options = p.anyscale(query.GetAnyscale())
		p.providerName = anyscaleParams.Name
	case *pb.GenerativeProvider_Aws:
		opts := query.GetAws()
		if opts.GetImageProperties() != nil {
			generative.Properties = append(generative.Properties, opts.GetImageProperties().Values...)
		}
		generative.Options = p.aws(opts)
		p.providerName = awsParams.Name
	case *pb.GenerativeProvider_Cohere:
		generative.Options = p.cohere(query.GetCohere())
		p.providerName = cohereParams.Name
	case *pb.GenerativeProvider_Mistral:
		generative.Options = p.mistral(query.GetMistral())
		p.providerName = mistralParams.Name
	case *pb.GenerativeProvider_Nvidia:
		generative.Options = p.nvidia(query.GetNvidia())
		p.providerName = nvidiaParams.Name
	case *pb.GenerativeProvider_Ollama:
		generative.Options = p.ollama(query.GetOllama())
		p.providerName = ollamaParams.Name
	case *pb.GenerativeProvider_Openai:
		opts := query.GetOpenai()
		if opts.GetImageProperties() != nil {
			generative.Properties = append(generative.Properties, opts.GetImageProperties().Values...)
		}
		generative.Options = p.openai(opts)
		p.providerName = openaiParams.Name
	case *pb.GenerativeProvider_Google:
		opts := query.GetGoogle()
		if opts.GetImageProperties() != nil {
			generative.Properties = append(generative.Properties, opts.GetImageProperties().Values...)
		}
		generative.Options = p.google(opts)
		p.providerName = googleParams.Name
	case *pb.GenerativeProvider_Databricks:
		generative.Options = p.databricks(query.GetDatabricks())
		p.providerName = databricksParams.Name
	case *pb.GenerativeProvider_Friendliai:
		generative.Options = p.friendliai(query.GetFriendliai())
		p.providerName = friendliaiParams.Name
	case *pb.GenerativeProvider_Xai:
		generative.Options = p.xai(query.GetXai())
		p.providerName = xaiParams.Name
	default:
		// do nothing
	}
	return query.ReturnMetadata
}

func (p *Parser) extract(req *pb.GenerativeSearch, class *models.Class) *generate.Params {
	generative := generate.Params{}
	if req.Single != nil {
		generative.Prompt = &req.Single.Prompt
		p.returnMetadata.single = p.extractFromQuery(&generative, req.Single.Queries)

		p.debug = req.Single.Debug
		generative.Debug = req.Single.Debug

		singleResultPrompts := generate.ExtractPropsFromPrompt(generative.Prompt)
		generative.PropertiesToExtract = append(generative.PropertiesToExtract, singleResultPrompts...)
	}
	if req.Grouped != nil {
		generative.Task = &req.Grouped.Task
		p.returnMetadata.grouped = p.extractFromQuery(&generative, req.Grouped.Queries) // populates generative.Properties with any values in provider.ImageProperties (if supported)
		if len(generative.Properties) == 0 && len(req.Grouped.GetProperties().GetValues()) == 0 {
			// if users do not supply any properties, all properties need to be extracted
			generative.PropertiesToExtract = append(generative.PropertiesToExtract, schema.GetPropertyNamesFromClass(class, false)...)
		} else {
			generative.Properties = append(generative.Properties, req.Grouped.Properties.GetValues()...)
			generative.PropertiesToExtract = append(generative.PropertiesToExtract, generative.Properties...)
		}
	}
	return &generative
}

func (p *Parser) anthropic(in *pb.GenerativeAnthropic) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		anthropicParams.Name: anthropicParams.Params{
			BaseURL:         in.GetBaseUrl(),
			Model:           in.GetModel(),
			Temperature:     in.Temperature,
			MaxTokens:       p.int64ToInt(in.MaxTokens),
			StopSequences:   in.StopSequences.GetValues(),
			TopP:            in.TopP,
			TopK:            p.int64ToInt(in.TopK),
			Images:          p.getStringPtrs(in.Images),
			ImageProperties: p.getStrings(in.ImageProperties),
		},
	}
}

func (p *Parser) anyscale(in *pb.GenerativeAnyscale) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		anyscaleParams.Name: anyscaleParams.Params{
			BaseURL:     in.GetBaseUrl(),
			Model:       in.GetModel(),
			Temperature: in.Temperature,
		},
	}
}

func (p *Parser) aws(in *pb.GenerativeAWS) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		awsParams.Name: awsParams.Params{
			Service:         in.GetService(),
			Region:          in.GetRegion(),
			Endpoint:        in.GetEndpoint(),
			TargetModel:     in.GetTargetModel(),
			TargetVariant:   in.GetTargetVariant(),
			Model:           in.GetModel(),
			Temperature:     in.Temperature,
			Images:          p.getStringPtrs(in.Images),
			ImageProperties: p.getStrings(in.ImageProperties),
		},
	}
}

func (p *Parser) cohere(in *pb.GenerativeCohere) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		cohereParams.Name: cohereParams.Params{
			BaseURL:          in.GetBaseUrl(),
			Model:            in.GetModel(),
			Temperature:      in.Temperature,
			MaxTokens:        p.int64ToInt(in.MaxTokens),
			K:                p.int64ToInt(in.K),
			P:                in.P,
			StopSequences:    in.StopSequences.GetValues(),
			FrequencyPenalty: in.FrequencyPenalty,
			PresencePenalty:  in.PresencePenalty,
		},
	}
}

func (p *Parser) mistral(in *pb.GenerativeMistral) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		mistralParams.Name: mistralParams.Params{
			BaseURL:     in.GetBaseUrl(),
			MaxTokens:   p.int64ToInt(in.MaxTokens),
			Model:       in.GetModel(),
			Temperature: in.Temperature,
			TopP:        in.TopP,
		},
	}
}

func (p *Parser) ollama(in *pb.GenerativeOllama) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		ollamaParams.Name: ollamaParams.Params{
			ApiEndpoint:     in.GetApiEndpoint(),
			Model:           in.GetModel(),
			Temperature:     in.Temperature,
			Images:          p.getStringPtrs(in.Images),
			ImageProperties: p.getStrings(in.ImageProperties),
		},
	}
}

func (p *Parser) openai(in *pb.GenerativeOpenAI) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		openaiParams.Name: openaiParams.Params{
			BaseURL:          in.GetBaseUrl(),
			ApiVersion:       in.GetApiVersion(),
			ResourceName:     in.GetResourceName(),
			DeploymentID:     in.GetDeploymentId(),
			IsAzure:          in.GetIsAzure(),
			Model:            in.GetModel(),
			FrequencyPenalty: in.FrequencyPenalty,
			MaxTokens:        p.int64ToInt(in.MaxTokens),
			N:                p.int64ToInt(in.N),
			PresencePenalty:  in.PresencePenalty,
			Stop:             in.Stop.GetValues(),
			Temperature:      in.Temperature,
			TopP:             in.TopP,
			Images:           p.getStringPtrs(in.Images),
			ImageProperties:  p.getStrings(in.ImageProperties),
		},
	}
}

func (p *Parser) google(in *pb.GenerativeGoogle) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		googleParams.Name: googleParams.Params{
			ApiEndpoint:      in.GetApiEndpoint(),
			ProjectID:        in.GetProjectId(),
			EndpointID:       in.GetEndpointId(),
			Region:           in.GetRegion(),
			Model:            in.GetModel(),
			Temperature:      in.Temperature,
			MaxTokens:        p.int64ToInt(in.MaxTokens),
			TopP:             in.TopP,
			TopK:             p.int64ToInt(in.TopK),
			StopSequences:    in.StopSequences.GetValues(),
			PresencePenalty:  in.PresencePenalty,
			FrequencyPenalty: in.FrequencyPenalty,
			Images:           p.getStringPtrs(in.Images),
			ImageProperties:  p.getStrings(in.ImageProperties),
		},
	}
}

func (p *Parser) databricks(in *pb.GenerativeDatabricks) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		databricksParams.Name: databricksParams.Params{
			Endpoint:         in.GetEndpoint(),
			Model:            in.GetModel(),
			FrequencyPenalty: in.FrequencyPenalty,
			Logprobs:         in.LogProbs,
			TopLogprobs:      p.int64ToInt(in.TopLogProbs),
			MaxTokens:        p.int64ToInt(in.MaxTokens),
			N:                p.int64ToInt(in.N),
			PresencePenalty:  in.PresencePenalty,
			Stop:             in.Stop.GetValues(),
			Temperature:      in.Temperature,
			TopP:             in.TopP,
		},
	}
}

func (p *Parser) friendliai(in *pb.GenerativeFriendliAI) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		friendliaiParams.Name: friendliaiParams.Params{
			BaseURL:     in.GetBaseUrl(),
			Model:       in.GetModel(),
			MaxTokens:   p.int64ToInt(in.MaxTokens),
			Temperature: in.Temperature,
			N:           p.int64ToInt(in.N),
			TopP:        in.TopP,
		},
	}
}

func (p *Parser) nvidia(in *pb.GenerativeNvidia) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		nvidiaParams.Name: nvidiaParams.Params{
			BaseURL:     in.GetBaseUrl(),
			Model:       in.GetModel(),
			Temperature: in.Temperature,
			TopP:        in.TopP,
			MaxTokens:   p.int64ToInt(in.MaxTokens),
		},
	}
}

func (p *Parser) xai(in *pb.GenerativeXAI) map[string]any {
	if in == nil {
		return nil
	}
	return map[string]any{
		xaiParams.Name: xaiParams.Params{
			BaseURL:         in.GetBaseUrl(),
			Model:           in.GetModel(),
			Temperature:     in.Temperature,
			TopP:            in.TopP,
			MaxTokens:       p.int64ToInt(in.MaxTokens),
			Images:          p.getStringPtrs(in.Images),
			ImageProperties: p.getStrings(in.ImageProperties),
		},
	}
}

func (p *Parser) getStringPtrs(in *pb.TextArray) []*string {
	if in != nil && len(in.Values) > 0 {
		vals := make([]*string, len(in.Values))
		for i, v := range in.Values {
			vals[i] = &v
		}
		return vals
	}
	return nil
}

func (p *Parser) getStrings(in *pb.TextArray) []string {
	if in != nil && len(in.Values) > 0 {
		return in.Values
	}
	return nil
}

func (p *Parser) int64ToInt(in *int64) *int {
	if in != nil && *in > 0 {
		out := int(*in)
		return &out
	}
	return nil
}
