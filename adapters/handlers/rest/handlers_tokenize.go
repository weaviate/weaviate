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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
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

	// Validate invertedIndexConfig with the same rules collection creation
	// applies, so the shape accepted here genuinely "mirrors the shape
	// accepted on a collection" (per the OpenAPI description). This also
	// defaults Stopwords.Preset to "en" when the caller sent an empty preset,
	// and rejects unknown presets, empty/whitespace preset names and empty
	// word lists the same way a collection create/update would.
	if params.Body.InvertedIndexConfig != nil {
		if err := inverted.ValidateConfig(params.Body.InvertedIndexConfig); err != nil {
			return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: fmt.Sprintf("invalid invertedIndexConfig: %s", err.Error())}},
			})
		}
	}

	// Build the stopword Provider, mirroring the collection-level configuration
	// the property-level endpoint inherits. Two optional request fields feed it:
	//   - invertedIndexConfig.stopwordPresets (map[string][]string, collection-shape)
	//   - stopwordPresets                      (map[string]StopwordConfig, richer tokenize-only form)
	// Top-level stopwordPresets wins on name conflict, matching the
	// "user-defined preset replaces built-in of same name" semantics we already
	// document at the collection level.
	var iicPresets map[string][]string
	if params.Body.InvertedIndexConfig != nil {
		iicPresets = params.Body.InvertedIndexConfig.StopwordPresets
	}
	presetDetectors, err := stopwords.BuildPresetDetectors(iicPresets)
	if err != nil {
		return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: err.Error()}},
		})
	}
	if len(params.Body.StopwordPresets) > 0 && presetDetectors == nil {
		presetDetectors = map[string]*stopwords.Detector{}
	}
	for name, cfg := range params.Body.StopwordPresets {
		// No explicit base → treat like "none" so additions/removals are
		// evaluated against an empty list (the same override semantics used
		// at the collection level).
		if cfg.Preset == "" {
			cfg.Preset = "none"
		}
		d, err := stopwords.NewDetectorFromConfig(cfg)
		if err != nil {
			return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: fmt.Sprintf("invalid stopwordPresets[%q]: %s", name, err.Error())}},
			})
		}
		presetDetectors[name] = d
	}

	// Collection-level fallback: invertedIndexConfig.stopwords is applied when
	// analyzerConfig.stopwordPreset is not set, matching the property endpoint
	// (which falls back to class.InvertedIndexConfig.Stopwords).
	var fallback stopwords.StopwordDetector
	var fallbackConfig *models.StopwordConfig
	if params.Body.InvertedIndexConfig != nil && params.Body.InvertedIndexConfig.Stopwords != nil {
		d, err := stopwords.NewDetectorFromConfig(*params.Body.InvertedIndexConfig.Stopwords)
		if err != nil {
			return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: fmt.Sprintf("invalid invertedIndexConfig.stopwords: %s", err.Error())}},
			})
		}
		fallback = d
		fallbackConfig = params.Body.InvertedIndexConfig.Stopwords
	}

	provider := stopwords.NewProvider(fallback, presetDetectors)

	// Resolve the detector using the same Provider semantics the property
	// endpoint uses: analyzerConfig.stopwordPreset plays the role of a
	// property-level preset override.
	var detector tokenizer.StopwordDetector
	var effectiveStopwords *models.StopwordConfig

	callerPreset := ""
	if params.Body.AnalyzerConfig != nil {
		callerPreset = params.Body.AnalyzerConfig.StopwordPreset
	}

	switch {
	case callerPreset != "":
		d, perr := provider.Get(&models.Property{TextAnalyzer: params.Body.AnalyzerConfig})
		if perr != nil {
			return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: fmt.Sprintf("unknown stopword preset %q; define it in invertedIndexConfig.stopwordPresets / stopwordPresets or use a built-in preset ('en', 'none')", callerPreset)}},
			})
		}
		detector = d
		effectiveStopwords = &models.StopwordConfig{Preset: callerPreset}
	case fallback != nil:
		detector = fallback
		effectiveStopwords = fallbackConfig
	case *params.Body.Tokenization == "word":
		// Default to "en" for word tokenization so the endpoint matches the
		// property-level endpoint's behavior when the collection's default
		// inverted index config is in effect. Route through the Provider so
		// a user override for "en" (in stopwordPresets or
		// invertedIndexConfig.stopwordPresets) is respected even when the
		// caller did not explicitly set analyzerConfig.stopwordPreset.
		d, perr := provider.Get(&models.Property{
			TextAnalyzer: &models.TextAnalyzerConfig{StopwordPreset: stopwords.EnglishPreset},
		})
		if perr != nil {
			return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: perr.Error()}},
			})
		}
		detector = d
		effectiveStopwords = &models.StopwordConfig{Preset: stopwords.EnglishPreset}
	}

	prepared := tokenizer.NewPreparedAnalyzer(params.Body.AnalyzerConfig)
	result := tokenizer.Analyze(*params.Body.Text, *params.Body.Tokenization, "", prepared, detector)

	// Surface the effective inverted-index configuration that produced the
	// query output, so callers can see which stopwords were applied.
	var invertedIndexConfig *models.InvertedIndexConfig
	if effectiveStopwords != nil {
		invertedIndexConfig = &models.InvertedIndexConfig{Stopwords: effectiveStopwords}
	}

	return tokenizeops.NewTokenizeOK().WithPayload(&models.TokenizeResponse{
		Tokenization:        *params.Body.Tokenization,
		AnalyzerConfig:      params.Body.AnalyzerConfig,
		InvertedIndexConfig: invertedIndexConfig,
		Indexed:             result.Indexed,
		Query:               result.Query,
	})
}

func propertyTokenize(params schemaops.SchemaObjectsPropertiesTokenizeParams,
	principal *models.Principal, schemaManager *schemaUC.Manager, logger logrus.FieldLogger,
) middleware.Responder {
	className := schema.UppercaseClassName(params.ClassName)

	// Resolve alias before authorization so authz uses the real collection name
	// for permissions and error UX (matches Handler.ShardsStatus).
	if resolved := schemaManager.ResolveAlias(className); resolved != "" {
		className = resolved
	}

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

	// Build a Provider from the collection-level stopword config and resolve
	// the property-level detector through it. This collapses the
	// per-property/preset/built-in/fallback resolution into a single Get call,
	// matching the production query path in adapters/repos/db/inverted.
	var fallback stopwords.StopwordDetector
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.Stopwords != nil {
		d, err := stopwords.NewDetectorFromConfig(*class.InvertedIndexConfig.Stopwords)
		if err != nil {
			logger.WithField("action", "create_stopword_detector").Error(err)
			return schemaops.NewSchemaObjectsPropertiesTokenizeInternalServerError().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: "failed to create stopword detector: " + err.Error()}},
			})
		}
		fallback = d
	}
	var presetDetectors map[string]*stopwords.Detector
	if class.InvertedIndexConfig != nil {
		d, err := stopwords.BuildPresetDetectors(class.InvertedIndexConfig.StopwordPresets)
		if err != nil {
			logger.WithField("action", "create_stopword_detector").Error(err)
			return schemaops.NewSchemaObjectsPropertiesTokenizeInternalServerError().WithPayload(&models.ErrorResponse{
				Error: []*models.ErrorResponseErrorItems0{{Message: "failed to create stopword detector: " + err.Error()}},
			})
		}
		presetDetectors = d
	}
	provider := stopwords.NewProvider(fallback, presetDetectors)
	detector, err := provider.Get(prop)
	if err != nil {
		// Property names a preset that is neither built-in nor user-defined.
		return schemaops.NewSchemaObjectsPropertiesTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: fmt.Sprintf("unknown stopword preset %q; must be a built-in preset ('en', 'none') or defined in invertedIndexConfig.stopwordPresets", prop.TextAnalyzer.StopwordPreset)}},
		})
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
