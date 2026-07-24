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
	"iter"
	"time"

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
	ignoreSet *IgnoreSet
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

// filterStopwords appends the tokens that are not stopwords to dst; a nil
// detector keeps every token. It is the one stopword-filter implementation —
// Analyze and AnalyzeBatch both run fold → tokenize → filterStopwords,
// differing only in buffer strategy.
func filterStopwords(dst []string, tokens []string, stopwords StopwordDetector) []string {
	for _, token := range tokens {
		if stopwords != nil && stopwords.IsStopword(token) {
			continue
		}
		dst = append(dst, token)
	}
	return dst
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
	indexed := TokenizeForClass(tokenization, prepared.foldText(text), className)
	query := filterStopwords(make([]string, 0, len(indexed)), indexed, stopwords)
	return AnalyzeResult{
		Indexed: indexed,
		Query:   query,
	}
}

// AnalyzedBatch holds the per-value query tokens produced by AnalyzeBatch.
// It is backed by two flat arrays — every token in value order plus one end
// offset per value — so a large batch avoids the per-value result allocation
// of calling Analyze in a loop (the shared backing buffers grow amortized);
// per-value views are computed on access, never stored.
//
// All views returned by Tokens and All alias the shared backing array and
// must be treated as read-only.
type AnalyzedBatch struct {
	flat []string
	// ends[i] is the end offset of value i's tokens in flat (value i's
	// tokens are flat[ends[i-1]:ends[i]]). uint32 caps a batch at 4B tokens,
	// far beyond any request size.
	ends []uint32
}

// Len returns the number of values in the batch.
func (b *AnalyzedBatch) Len() int {
	return len(b.ends)
}

// Tokens returns value i's query tokens; the result is empty when value i
// was entirely stopword-filtered (or the tokenization was unknown).
func (b *AnalyzedBatch) Tokens(i int) []string {
	start := uint32(0)
	if i > 0 {
		start = b.ends[i-1]
	}
	end := b.ends[i]
	// full slice expression: a caller appending to one value's view cannot
	// clobber its neighbor's tokens in the shared backing array
	return b.flat[start:end:end]
}

// All iterates the batch in value order, yielding each value's index and
// query tokens: for i, tokens := range batch.All() { ... }.
func (b *AnalyzedBatch) All() iter.Seq2[int, []string] {
	return func(yield func(int, []string) bool) {
		start := uint32(0)
		for i, end := range b.ends {
			if !yield(i, b.flat[start:end:end]) {
				return
			}
			start = end
		}
	}
}

// AnalyzeBatch analyzes each value independently — the batch equivalent of
// calling Analyze per value and collecting each result's Query — but records
// tokenizer metrics once for the whole batch (one duration observation, the
// summed pre-stopword token count) and reuses buffers across values, so the
// per-value cost is the tokenization itself rather than metric and
// allocation scaffolding. The whole batch counts as a single
// TokenCountPerRequest observation with the summed token count, where the
// per-value path observes once per value.
//
// Offsets are uint32: a single batch must stay under 4B tokens, which any
// request-bounded caller does by orders of magnitude; exceeding it silently
// corrupts per-value views.
//
// Per-value Indexed is intentionally not exposed: materializing it would
// force a per-value allocation, defeating the batch — callers needing
// Indexed should use Analyze.
func AnalyzeBatch(
	values []string,
	tokenization string,
	className string,
	prepared *PreparedAnalyzer,
	stopwords StopwordDetector,
) *AnalyzedBatch {
	batch := &AnalyzedBatch{ends: make([]uint32, len(values))}
	if len(values) == 0 {
		return batch
	}

	// dispatch once for the whole batch; fn == nil (unknown tokenization or
	// unavailable tokenizer) leaves every value's tokens empty (all end
	// offsets zero) and records no metrics, matching the per-value path's
	// guard early-outs
	fn, throttled := resolveTokenizer(tokenization, className)
	if fn == nil {
		return batch
	}

	totalTokens := 0
	flat := make([]string, 0, len(values))
	var scratch []string // one value's raw tokens, reused across the batch
	// busy is the in-slot analysis time, excluding waits on re-acquiring the
	// throttle between chunks — the same semantics as the per-value path,
	// which acquires before its duration window starts.
	var busy time.Duration

	// processChunk analyzes values[from:to] under one throttle acquisition
	// (when the tokenization is throttled at all), released panic-safe via
	// defer.
	processChunk := func(from, to int) {
		if throttled {
			ApacTokenizerThrottle <- struct{}{}
			defer func() { <-ApacTokenizerThrottle }()
		}
		chunkStart := time.Now()
		defer func() { busy += time.Since(chunkStart) }()
		for i := from; i < to; i++ {
			scratch = fn(prepared.foldText(values[i]), scratch[:0])
			totalTokens += len(scratch)
			flat = filterStopwords(flat, scratch, stopwords)
			batch.ends[i] = uint32(len(flat))
		}
	}

	// An unthrottled batch runs as a single chunk. A throttled one yields its
	// slot every throttledBatchChunk values, so concurrent large batches
	// cannot hold every throttle slot for their whole duration and starve
	// single-value tokenizations — waiters get in between chunks.
	chunk := len(values)
	if throttled {
		chunk = throttledBatchChunk
	}
	for from := 0; from < len(values); from += chunk {
		processChunk(from, min(from+chunk, len(values)))
	}
	batch.flat = flat

	// One record for the whole batch, under the same label the per-value path
	// uses. As in Analyze, the token count is the indexed (pre-stopword)
	// count; the duration is the summed in-slot analysis time.
	metricsFor(tokenization).record(busy, totalTokens)
	return batch
}

// throttledBatchChunk bounds how many values a batch analyzes per
// ApacTokenizerThrottle acquisition. Throttled kernels cost roughly 0.1-1ms
// per value, so 16 keeps the worst-case slot hold in the low milliseconds
// for waiting single-value tokenizations, while the ~µs acquire/release cost
// per chunk stays negligible.
const throttledBatchChunk = 16

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
