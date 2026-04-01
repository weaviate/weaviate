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

package tokenizer

import (
	"github.com/weaviate/weaviate/entities/models"
)

// StopwordDetector is satisfied by stopwords.Detector and test fakes.
type StopwordDetector interface {
	IsStopword(word string) bool
}

// AnalyzeResult holds the output of Analyze: the indexed tokens and the
// query tokens (indexed minus stopwords).
type AnalyzeResult struct {
	Indexed []string
	Query   []string
}

// PreparedAnalyzer caches the ignore set built from a TextAnalyzerConfig so
// that repeated Analyze calls (e.g. over a text array) don't rebuild it each
// time.  Create one via NewPreparedAnalyzer; when the schema is updated a new
// PreparedAnalyzer should be created.
type PreparedAnalyzer struct {
	fold      bool
	ignoreSet map[rune]struct{}
}

// NewPreparedAnalyzer pre-builds the ignore set from the given config.
// Returns nil when no folding is configured, which Analyze handles as a no-op.
func NewPreparedAnalyzer(cfg *models.TextAnalyzerConfig) *PreparedAnalyzer {
	if cfg == nil || !cfg.ASCIIFold {
		return nil
	}
	return &PreparedAnalyzer{
		fold:      true,
		ignoreSet: BuildIgnoreSet(cfg.ASCIIFoldIgnore),
	}
}

// foldText applies ASCII folding if configured.
func (p *PreparedAnalyzer) foldText(text string) string {
	if p == nil || !p.fold {
		return text
	}
	return FoldASCII(text, p.ignoreSet)
}

// Analyze runs the full text-analysis pipeline: ASCII-fold → tokenize →
// stopword removal (query only).
//
// The PreparedAnalyzer may be nil (no folding). Create one via
// NewPreparedAnalyzer to reuse across multiple calls with the same config.
func Analyze(
	text string,
	tokenization string,
	className string,
	prepared *PreparedAnalyzer,
	stopwords StopwordDetector,
) AnalyzeResult {
	text = prepared.foldText(text)

	indexed := TokenizeForClass(tokenization, text, className)

	query := make([]string, 0, len(indexed))
	for _, token := range indexed {
		if stopwords != nil && stopwords.IsStopword(token) {
			continue
		}
		query = append(query, token)
	}

	return AnalyzeResult{
		Indexed: indexed,
		Query:   query,
	}
}

// AnalyzeAndCountDuplicates is like Analyze but also deduplicates tokens and
// returns per-token counts (boost factors). Used by BM25 scoring.
func AnalyzeAndCountDuplicates(
	text string,
	tokenization string,
	className string,
	prepared *PreparedAnalyzer,
	stopwords StopwordDetector,
) (terms []string, boosts []int) {
	text = prepared.foldText(text)

	raw := TokenizeForClass(tokenization, text, className)

	counts := map[string]int{}
	for _, term := range raw {
		if stopwords != nil && stopwords.IsStopword(term) {
			continue
		}
		counts[term]++
	}

	terms = make([]string, 0, len(counts))
	boosts = make([]int, 0, len(counts))
	for term, boost := range counts {
		terms = append(terms, term)
		boosts = append(boosts, boost)
	}
	return terms, boosts
}
