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

// Analyze runs the full text-analysis pipeline used for both indexing and
// querying: ASCII-fold → tokenize → stopword removal (query only).
//
// Parameters:
//   - text: the raw input string
//   - tokenization: one of the PropertyTokenization* constants
//   - className: the collection name (needed for per-class custom tokenizers)
//   - TextAnalyzer: per-property folding config (may be nil)
//   - stopwords: stopword detector for the collection (may be nil)
func Analyze(
	text string,
	tokenization string,
	className string,
	TextAnalyzer *models.TextAnalyzerConfig,
	stopwords StopwordDetector,
) AnalyzeResult {
	if TextAnalyzer != nil && TextAnalyzer.ASCIIFold {
		ignore := BuildIgnoreSet(TextAnalyzer.ASCIIFoldIgnore)
		text = FoldASCII(text, ignore)
	}

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
	TextAnalyzer *models.TextAnalyzerConfig,
	stopwords StopwordDetector,
) (terms []string, boosts []int) {
	if TextAnalyzer != nil && TextAnalyzer.ASCIIFold {
		ignore := BuildIgnoreSet(TextAnalyzer.ASCIIFoldIgnore)
		text = FoldASCII(text, ignore)
	}

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
