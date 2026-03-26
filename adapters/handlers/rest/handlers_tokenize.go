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

package rest

import (
	"strings"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	schemaops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	tokenizeops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/tokenize"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

func setupTokenizeHandlers(api *operations.WeaviateAPI, schemaManager *schemaUC.Manager, logger logrus.FieldLogger) {
	api.TokenizeTokenizeHandler = tokenizeops.TokenizeHandlerFunc(
		func(params tokenizeops.TokenizeParams, principal *models.Principal) middleware.Responder {
			return handleGenericTokenize(params, logger)
		})

	api.SchemaSchemaObjectsPropertiesTokenizeHandler = schemaops.SchemaObjectsPropertiesTokenizeHandlerFunc(
		func(params schemaops.SchemaObjectsPropertiesTokenizeParams, principal *models.Principal) middleware.Responder {
			return handlePropertyTokenize(params, schemaManager, logger)
		})
}

func handleGenericTokenize(params tokenizeops.TokenizeParams, logger logrus.FieldLogger) middleware.Responder {
	indexed := tokenizer.Tokenize(*params.Body.Tokenization, *params.Body.Text)
	query := make([]string, len(indexed))
	copy(query, indexed)

	var analyzerConfig *models.TokenizeAnalyzerConfig
	if params.Body.AnalyzerConfig != nil && params.Body.AnalyzerConfig.Stopwords != nil {
		analyzerConfig = params.Body.AnalyzerConfig
		detector, err := stopwords.NewDetectorFromConfig(*params.Body.AnalyzerConfig.Stopwords)
		if err != nil {
			return tokenizeops.NewTokenizeBadRequest()
		}
		query = filterStopwords(indexed, detector)
	}

	return tokenizeops.NewTokenizeOK().WithPayload(&models.TokenizeResponse{
		Tokenization:   *params.Body.Tokenization,
		AnalyzerConfig: analyzerConfig,
		Indexed:        indexed,
		Query:          query,
	})
}

func handlePropertyTokenize(params schemaops.SchemaObjectsPropertiesTokenizeParams,
	schemaManager *schemaUC.Manager, logger logrus.FieldLogger,
) middleware.Responder {
	className := schema.UppercaseClassName(params.ClassName)
	class := schemaManager.ReadOnlyClass(className)
	if class == nil {
		return schemaops.NewSchemaObjectsPropertiesTokenizeNotFound()
	}

	var prop *models.Property
	for _, p := range class.Properties {
		if strings.EqualFold(p.Name, params.PropertyName) {
			prop = p
			break
		}
	}
	if prop == nil {
		return schemaops.NewSchemaObjectsPropertiesTokenizeNotFound()
	}

	if prop.Tokenization == "" {
		return schemaops.NewSchemaObjectsPropertiesTokenizeBadRequest()
	}

	indexed := tokenizer.TokenizeForClass(prop.Tokenization, *params.Body.Text, className)
	query := make([]string, len(indexed))
	copy(query, indexed)

	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.Stopwords != nil {
		detector, err := stopwords.NewDetectorFromConfig(*class.InvertedIndexConfig.Stopwords)
		if err != nil {
			logger.WithField("action", "create_stopword_detector").Error(err)
		} else {
			query = filterStopwords(indexed, detector)
		}
	}

	return schemaops.NewSchemaObjectsPropertiesTokenizeOK().WithPayload(&models.TokenizeResponse{
		Tokenization: prop.Tokenization,
		Indexed:      indexed,
		Query:        query,
	})
}

func filterStopwords(tokens []string, detector *stopwords.Detector) []string {
	filtered := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if !detector.IsStopword(token) {
			filtered = append(filtered, token)
		}
	}
	return filtered
}
