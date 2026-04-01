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

// AnalyseResult holds the output of Analyse: the indexed tokens and the
// query tokens (indexed minus stopwords).
type AnalyseResult struct {
	Indexed []string
	Query   []string
}

// Analyse runs the full text-analysis pipeline used for both indexing and
// querying: ASCII-fold → tokenize → stopword removal (query only).
//
// Parameters:
//   - text: the raw input string
//   - tokenization: one of the PropertyTokenization* constants
//   - className: the collection name (needed for per-class custom tokenizers)
//   - textAnalyser: per-property folding config (may be nil)
//   - stopwords: stopword detector for the collection (may be nil)
func Analyse(
	text string,
	tokenization string,
	className string,
	textAnalyser *models.TextAnalyserConfig,
	stopwords StopwordDetector,
) AnalyseResult {
	if textAnalyser != nil && textAnalyser.ASCIIFold {
		ignore := BuildIgnoreSet(textAnalyser.ASCIIFoldIgnore)
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

	return AnalyseResult{
		Indexed: indexed,
		Query:   query,
	}
}

// AnalyseAndCountDuplicates is like Analyse but also deduplicates tokens and
// returns per-token counts (boost factors). Used by BM25 scoring.
func AnalyseAndCountDuplicates(
	text string,
	tokenization string,
	className string,
	textAnalyser *models.TextAnalyserConfig,
	stopwords StopwordDetector,
) (terms []string, boosts []int) {
	if textAnalyser != nil && textAnalyser.ASCIIFold {
		ignore := BuildIgnoreSet(textAnalyser.ASCIIFoldIgnore)
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
