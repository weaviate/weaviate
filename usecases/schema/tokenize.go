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

package schema

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

// TokenizeResult is the indexed/query token output of a tokenize call.
type TokenizeResult struct {
	Indexed []string
	Query   []string
}

// TokenizeError categorises tokenize failures so adapters can map them to the
// correct transport status (HTTP 4xx vs 5xx, gRPC InvalidArgument vs Internal).
type TokenizeError struct {
	Kind TokenizeErrorKind
	Msg  string
}

func (e *TokenizeError) Error() string { return e.Msg }

type TokenizeErrorKind int

const (
	// TokenizeErrInvalid covers caller mistakes (bad config, unknown preset,
	// disabled tokenization). Adapters should surface 422 / InvalidArgument.
	TokenizeErrInvalid TokenizeErrorKind = iota
	// TokenizeErrNotFound is returned when the property does not exist on the
	// class. Adapters should surface 404 / NotFound.
	TokenizeErrNotFound
	// TokenizeErrInternal covers unexpected detector-construction failures.
	// Adapters should surface 500 / Internal.
	TokenizeErrInternal
)

func newInvalidErr(format string, args ...any) *TokenizeError {
	return &TokenizeError{Kind: TokenizeErrInvalid, Msg: fmt.Sprintf(format, args...)}
}

func newNotFoundErr(format string, args ...any) *TokenizeError {
	return &TokenizeError{Kind: TokenizeErrNotFound, Msg: fmt.Sprintf(format, args...)}
}

func newInternalErr(format string, args ...any) *TokenizeError {
	return &TokenizeError{Kind: TokenizeErrInternal, Msg: fmt.Sprintf(format, args...)}
}

// PropertyTokenize tokenizes text using a property's configured tokenizer
// and the collection-level stopwords/presets. The class must already be
// resolved (alias resolution and authorization are caller responsibilities).
func PropertyTokenize(class *models.Class, propertyName, text string) (*TokenizeResult, error) {
	if class == nil {
		return nil, newNotFoundErr("class not found")
	}

	var prop *models.Property
	for _, p := range class.Properties {
		if strings.EqualFold(p.Name, propertyName) {
			prop = p
			break
		}
	}
	if prop == nil {
		return nil, newNotFoundErr("property %q not found on class %q", propertyName, class.Class)
	}
	if prop.Tokenization == "" {
		return nil, newInvalidErr("tokenization is not enabled for this property")
	}

	provider, err := buildProvider(class.InvertedIndexConfig)
	if err != nil {
		return nil, err
	}

	detector, perr := provider.Get(prop)
	if perr != nil {
		preset := ""
		if prop.TextAnalyzer != nil {
			preset = prop.TextAnalyzer.StopwordPreset
		}
		return nil, newInvalidErr(
			"unknown stopword preset %q; must be a built-in preset ('en', 'none') or defined in invertedIndexConfig.stopwordPresets",
			preset,
		)
	}

	prepared := tokenizer.NewPreparedAnalyzer(prop.TextAnalyzer)
	res := tokenizer.Analyze(text, prop.Tokenization, class.Class, prepared, detector)
	return &TokenizeResult{Indexed: res.Indexed, Query: res.Query}, nil
}

// GenericTokenize tokenizes text using a request-supplied tokenization
// strategy and inline stopwords/presets, without referencing any collection
// schema. The shape accepted here matches the shape accepted on a collection.
func GenericTokenize(
	text, tokenization string,
	analyzerCfg *models.TextAnalyzerConfig,
	stopwordsCfg *models.StopwordConfig,
	presets map[string][]string,
) (*TokenizeResult, error) {
	if !slices.Contains(tokenizer.Tokenizations, tokenization) {
		return nil, newInvalidErr("unsupported tokenization strategy: %s", tokenization)
	}
	// Cap input length to prevent abuse — tokenizer can handle more, but it
	// may degrade performance on this synchronous endpoint.
	if len(text) > 10000 {
		return nil, newInvalidErr("text exceeds maximum allowed length of 10,000 characters")
	}
	if err := validateAnalyzerConfig(analyzerCfg); err != nil {
		return nil, newInvalidErr("%s", err.Error())
	}
	// stopwords and stopwordPresets are mutually exclusive: stopwords selects
	// one base preset (optionally tweaked); stopwordPresets defines named
	// presets selected via analyzerConfig.stopwordPreset. Allowing both
	// creates ambiguous resolution corner cases.
	if stopwordsCfg != nil && len(presets) > 0 {
		return nil, newInvalidErr("stopwords and stopwordPresets are mutually exclusive; pass only one")
	}

	// Validate with the same rules collection creation applies, so the request
	// shape genuinely matches the shape accepted on a collection. This also
	// defaults Stopwords.Preset to "en" when the caller sent an empty preset.
	synthConfig := &models.InvertedIndexConfig{Stopwords: stopwordsCfg, StopwordPresets: presets}
	if err := inverted.ValidateConfig(synthConfig); err != nil {
		return nil, newInvalidErr("%s", err.Error())
	}

	presetDetectors, err := stopwords.BuildPresetDetectors(presets)
	if err != nil {
		return nil, newInvalidErr("%s", err.Error())
	}

	var fallback stopwords.StopwordDetector
	if stopwordsCfg != nil {
		d, derr := stopwords.NewDetectorFromConfig(*stopwordsCfg)
		if derr != nil {
			return nil, newInvalidErr("invalid stopwords: %s", derr.Error())
		}
		fallback = d
	}

	provider := stopwords.NewProvider(fallback, presetDetectors)

	var detector tokenizer.StopwordDetector
	callerPreset := ""
	if analyzerCfg != nil {
		callerPreset = analyzerCfg.StopwordPreset
	}
	switch {
	case callerPreset != "":
		// analyzerConfig.stopwordPreset plays the role of a property-level
		// preset override: route through the Provider so user-defined presets
		// win over built-ins, matching collection-create override semantics.
		d, perr := provider.Get(&models.Property{TextAnalyzer: analyzerCfg})
		if perr != nil {
			return nil, newInvalidErr(
				"unknown stopword preset %q; define it in stopwordPresets or use a built-in preset ('en', 'none')",
				callerPreset,
			)
		}
		detector = d
	case fallback != nil:
		detector = fallback
	case tokenization == "word":
		// Default to "en" for word tokenization so the endpoint matches the
		// property-level endpoint's behavior under the default inverted index
		// config. Route through the Provider so a user override for "en" in
		// stopwordPresets is respected even without an explicit preset.
		d, perr := provider.Get(&models.Property{
			TextAnalyzer: &models.TextAnalyzerConfig{StopwordPreset: stopwords.EnglishPreset},
		})
		if perr != nil {
			return nil, newInvalidErr("%s", perr.Error())
		}
		detector = d
	}

	prepared := tokenizer.NewPreparedAnalyzer(analyzerCfg)
	res := tokenizer.Analyze(text, tokenization, "", prepared, detector)
	return &TokenizeResult{Indexed: res.Indexed, Query: res.Query}, nil
}

func buildProvider(ic *models.InvertedIndexConfig) (*stopwords.Provider, *TokenizeError) {
	var fallback stopwords.StopwordDetector
	if ic != nil && ic.Stopwords != nil {
		d, err := stopwords.NewDetectorFromConfig(*ic.Stopwords)
		if err != nil {
			return nil, newInternalErr("failed to create stopword detector: %s", err.Error())
		}
		fallback = d
	}
	var presetDetectors map[string]*stopwords.Detector
	if ic != nil {
		d, err := stopwords.BuildPresetDetectors(ic.StopwordPresets)
		if err != nil {
			return nil, newInternalErr("failed to create stopword detector: %s", err.Error())
		}
		presetDetectors = d
	}
	return stopwords.NewProvider(fallback, presetDetectors), nil
}

func validateAnalyzerConfig(cfg *models.TextAnalyzerConfig) error {
	if cfg == nil {
		return nil
	}
	if !cfg.ASCIIFold && len(cfg.ASCIIFoldIgnore) > 0 {
		return errors.New("asciiFoldIgnore requires asciiFold to be enabled")
	}
	for _, entry := range cfg.ASCIIFoldIgnore {
		if utf8.RuneCountInString(norm.NFC.String(entry)) != 1 {
			return fmt.Errorf("each asciiFoldIgnore entry must be a single character, got %q", entry)
		}
	}
	return nil
}
