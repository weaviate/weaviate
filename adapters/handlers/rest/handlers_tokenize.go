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
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"golang.org/x/text/unicode/norm"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	schemaops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	tokenizeops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/tokenize"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

func setupTokenizeHandlers(api *operations.WeaviateAPI, schemaManager *schemaUC.Manager, logger logrus.FieldLogger) {
	api.TokenizeTokenizeHandler = tokenizeops.TokenizeHandlerFunc(
		func(params tokenizeops.TokenizeParams, principal *models.Principal) middleware.Responder {
			return genericTokenize(params)
		})

	api.SchemaSchemaObjectsPropertiesTokenizeHandler = schemaops.SchemaObjectsPropertiesTokenizeHandlerFunc(
		func(params schemaops.SchemaObjectsPropertiesTokenizeParams, principal *models.Principal) middleware.Responder {
			return propertyTokenize(params, principal, schemaManager, logger)
		})
}

func genericTokenize(params tokenizeops.TokenizeParams) middleware.Responder {
	if !slices.Contains(tokenizer.Tokenizations, *params.Body.Tokenization) {
		return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: "unsupported tokenization strategy: " + *params.Body.Tokenization}},
		})
	}

	// allow a max length of 10k characters to prevent abuse of this endpoint; the tokenizer can handle more, but it may cause performance issues
	if len(*params.Body.Text) > 10000 {
		return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: "text exceeds maximum allowed length of 10,000 characters"}},
		})
	}

	if err := validateAnalyzerConfig(params.Body.AnalyzerConfig); err != nil {
		return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: err.Error()}},
		})
	}

	prepared := tokenizer.NewPreparedAnalyzer(params.Body.AnalyzerConfig)

	var detector tokenizer.StopwordDetector
	if swCfg := params.Body.StopwordConfig; swCfg != nil {
		if swCfg.Preset == "" {
			swCfg.Preset = "none"
		}
		var err error
		detector, err = stopwords.NewDetectorFromConfig(*swCfg)
		if err != nil {
			return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: "invalid stopword configuration: " + err.Error()}},
			})
		}
	}

	result := tokenizer.Analyze(*params.Body.Text, *params.Body.Tokenization, "", prepared, detector)

	return tokenizeops.NewTokenizeOK().WithPayload(&models.TokenizeResponse{
		Tokenization:   *params.Body.Tokenization,
		AnalyzerConfig: params.Body.AnalyzerConfig,
		StopwordConfig: params.Body.StopwordConfig,
		Indexed:        result.Indexed,
		Query:          result.Query,
	})
}

func propertyTokenize(params schemaops.SchemaObjectsPropertiesTokenizeParams,
	principal *models.Principal, schemaManager *schemaUC.Manager, logger logrus.FieldLogger,
) middleware.Responder {
	className := schema.UppercaseClassName(params.ClassName)

	// Authorize: reading collection metadata (same as other schema read operations)
	err := schemaManager.Authorizer.Authorize(
		params.HTTPRequest.Context(), principal, authorization.READ,
		authorization.CollectionsMetadata(className)...,
	)
	if err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schemaops.NewSchemaObjectsPropertiesTokenizeForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		}
		return schemaops.NewSchemaObjectsPropertiesTokenizeInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

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
		return schemaops.NewSchemaObjectsPropertiesTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: "tokenization is not enabled for this property"}},
		})
	}

	var detector tokenizer.StopwordDetector
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.Stopwords != nil {
		var err error
		detector, err = stopwords.NewDetectorFromConfig(*class.InvertedIndexConfig.Stopwords)
		if err != nil {
			logger.WithField("action", "create_stopword_detector").Error(err)
			return schemaops.NewSchemaObjectsPropertiesTokenizeInternalServerError().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: "failed to create stopword detector: " + err.Error()}},
			})
		}
	}

	prepared := tokenizer.NewPreparedAnalyzer(prop.TextAnalyzer)
	result := tokenizer.Analyze(*params.Body.Text, prop.Tokenization, className, prepared, detector)

	return schemaops.NewSchemaObjectsPropertiesTokenizeOK().WithPayload(&models.TokenizeResponse{
		Tokenization: prop.Tokenization,
		Indexed:      result.Indexed,
		Query:        result.Query,
	})
}

func validateAnalyzerConfig(cfg *models.TextAnalyzerConfig) error {
	if cfg == nil {
		return nil
	}
	if !cfg.ASCIIFold && len(cfg.ASCIIFoldIgnore) > 0 {
		return fmt.Errorf("asciiFoldIgnore requires asciiFold to be enabled")
	}
	for _, entry := range cfg.ASCIIFoldIgnore {
		if utf8.RuneCountInString(norm.NFC.String(entry)) != 1 {
			return fmt.Errorf("each asciiFoldIgnore entry must be a single character, got %q", entry)
		}
	}
	return nil
}
