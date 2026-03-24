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
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type tokenizeRequest struct {
	Text           string          `json:"text"`
	Tokenization   string          `json:"tokenization"`
	AnalyzerConfig *analyzerConfig `json:"analyzerConfig,omitempty"`
}

type analyzerConfig struct {
	Stopwords *models.StopwordConfig `json:"stopwords,omitempty"`
}

type tokenizeResponse struct {
	Tokenization   string          `json:"tokenization"`
	AnalyzerConfig *analyzerConfig `json:"analyzerConfig,omitempty"`
	Indexed        []string        `json:"indexed"`
	Query          []string        `json:"query"`
}

type propertyTokenizeRequest struct {
	Text string `json:"text"`
}

var propertyTokenizePath = regexp.MustCompile(`^/v1/schema/([^/]+)/properties/([^/]+)/tokenize$`)

func addTokenizeHandlers(schemaManager *schemaUC.Manager, logger logrus.FieldLogger, next http.Handler) http.Handler {
	log := logger.WithField("handler", "tokenize")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/tokenize" {
			handleGenericTokenize(w, r, log)
			return
		}

		if r.Method == http.MethodPost {
			if matches := propertyTokenizePath.FindStringSubmatch(r.URL.Path); matches != nil {
				handlePropertyTokenize(w, r, schemaManager, matches[1], matches[2], log)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func handleGenericTokenize(w http.ResponseWriter, r *http.Request, logger logrus.FieldLogger) {
	var req tokenizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.Text == "" {
		http.Error(w, "text is required", http.StatusBadRequest)
		return
	}
	if req.Tokenization == "" {
		http.Error(w, "tokenization is required", http.StatusBadRequest)
		return
	}
	if !slices.Contains(tokenizer.Tokenizations, req.Tokenization) {
		http.Error(w, fmt.Sprintf("unknown tokenization %q, available: %v", req.Tokenization, tokenizer.Tokenizations), http.StatusBadRequest)
		return
	}

	indexed := tokenizer.Tokenize(req.Tokenization, req.Text)
	query := make([]string, len(indexed))
	copy(query, indexed)

	if req.AnalyzerConfig != nil && req.AnalyzerConfig.Stopwords != nil {
		detector, err := stopwords.NewDetectorFromConfig(*req.AnalyzerConfig.Stopwords)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid stopwords config: %v", err), http.StatusBadRequest)
			return
		}
		query = filterStopwords(indexed, detector)
	}

	resp := tokenizeResponse{
		Tokenization:   req.Tokenization,
		AnalyzerConfig: req.AnalyzerConfig,
		Indexed:        indexed,
		Query:          query,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.WithField("action", "encode_response").Error(err)
	}
}

func handlePropertyTokenize(w http.ResponseWriter, r *http.Request, schemaManager *schemaUC.Manager,
	className, propertyName string, logger logrus.FieldLogger,
) {
	var req propertyTokenizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.Text == "" {
		http.Error(w, "text is required", http.StatusBadRequest)
		return
	}

	className = schema.UppercaseClassName(className)
	class := schemaManager.ReadOnlyClass(className)
	if class == nil {
		http.Error(w, fmt.Sprintf("class %q not found", className), http.StatusNotFound)
		return
	}

	var prop *models.Property
	for _, p := range class.Properties {
		if strings.EqualFold(p.Name, propertyName) {
			prop = p
			break
		}
	}
	if prop == nil {
		http.Error(w, fmt.Sprintf("property %q not found in class %q", propertyName, className), http.StatusNotFound)
		return
	}

	if prop.Tokenization == "" {
		http.Error(w, fmt.Sprintf("property %q does not have tokenization configured", propertyName), http.StatusBadRequest)
		return
	}

	indexed := tokenizer.TokenizeForClass(prop.Tokenization, req.Text, className)
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

	resp := tokenizeResponse{
		Tokenization: prop.Tokenization,
		Indexed:      indexed,
		Query:        query,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.WithField("action", "encode_response").Error(err)
	}
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
