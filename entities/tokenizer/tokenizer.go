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
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	entcfg "github.com/weaviate/weaviate/entities/config"

	"github.com/go-ego/gse"
	koDict "github.com/ikawaha/kagome-dict-ko"
	"github.com/ikawaha/kagome-dict/dict"
	"github.com/ikawaha/kagome-dict/ipa"
	kagomeTokenizer "github.com/ikawaha/kagome/v2/tokenizer"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

var (
	gseTokenizer   *gse.Segmenter  // Japanese
	gseTokenizerCh *gse.Segmenter  // Chinese
	gseLock        = &sync.Mutex{} // Lock for gse
	UseGse         = false         // Load Japanese dictionary and prepare tokenizer
	UseGseCh       = false         // Load Chinese dictionary and prepare tokenizer
	// The Tokenizer Libraries can consume a lot of memory, so we limit the number of parallel tokenizers
	ApacTokenizerThrottle = chan struct{}(nil) // Throttle for tokenizers
	tokenizers            KagomeTokenizers     // Tokenizers for Korean and Japanese
	kagomeInitLock        sync.Mutex           // Lock for kagome initialization

	customTokenizers sync.Map
)

type KagomeTokenizers struct {
	Korean   *kagomeTokenizer.Tokenizer
	Japanese *kagomeTokenizer.Tokenizer
}

// Optional tokenizers can be enabled with an environment variable like:
// 'ENABLE_TOKENIZER_XXX', e.g. 'ENABLE_TOKENIZER_GSE', 'ENABLE_TOKENIZER_KAGOME_KR', 'ENABLE_TOKENIZER_KAGOME_JA'
var Tokenizations []string = []string{
	models.PropertyTokenizationWord,
	models.PropertyTokenizationLowercase,
	models.PropertyTokenizationWhitespace,
	models.PropertyTokenizationField,
	models.PropertyTokenizationTrigram,
}

func init() {
	ApacTokenizerThrottle = make(chan struct{}, throttleCapacity(os.Getenv("TOKENIZER_CONCURRENCY_COUNT")))
	InitOptionalTokenizers()
	customTokenizers = sync.Map{}
}

// acquireThrottleSlot blocks for an ApacTokenizerThrottle slot and returns
// its release: defer acquireThrottleSlot()().
func acquireThrottleSlot() func() {
	ApacTokenizerThrottle <- struct{}{}
	return func() { <-ApacTokenizerThrottle }
}

// throttleCapacity computes the ApacTokenizerThrottle capacity from the
// TOKENIZER_CONCURRENCY_COUNT env value. Values below 1 or non-numeric fall
// back to GOMAXPROCS with a warning (capacity 0 would deadlock, negative panics).
func throttleCapacity(envValue string) int {
	fallback := runtime.GOMAXPROCS(0)
	if envValue == "" {
		return fallback
	}
	x, err := strconv.Atoi(envValue)
	if err != nil {
		logrus.StandardLogger().Warnf("invalid TOKENIZER_CONCURRENCY_COUNT %q, using default %d: %v", envValue, fallback, err)
		return fallback
	}
	if x < 1 {
		logrus.StandardLogger().Warnf("invalid TOKENIZER_CONCURRENCY_COUNT %d, must be at least 1, using default %d", x, fallback)
		return fallback
	}
	return x
}

func InitOptionalTokenizers() {
	logger := logrus.StandardLogger().WithField("action", "tokenizer_init")
	if entcfg.Enabled(os.Getenv("USE_GSE")) || entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_GSE")) {
		if err := init_gse(); err != nil {
			logger.WithField("tokenizer", "gse").Error(err)
		}
	}
	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_GSE_CH")) {
		if err := init_gse_ch(); err != nil {
			logger.WithField("tokenizer", "gse_ch").Error(err)
		}
	}
	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_KR")) && tokenizers.Korean == nil {
		if err := func() error {
			kagomeInitLock.Lock()
			defer kagomeInitLock.Unlock()
			var err error
			tokenizers.Korean, err = initializeKagomeTokenizerKr(nil)
			if err != nil {
				return err
			}
			enableTokenization(models.PropertyTokenizationKagomeKr)
			return nil
		}(); err != nil {
			logger.WithField("tokenizer", "kagome_kr").Error(err)
		}
	}
	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_JA")) && tokenizers.Japanese == nil {
		if err := func() error {
			kagomeInitLock.Lock()
			defer kagomeInitLock.Unlock()
			var err error
			tokenizers.Japanese, err = initializeKagomeTokenizerJa(nil)
			if err != nil {
				return err
			}
			enableTokenization(models.PropertyTokenizationKagomeJa)
			return nil
		}(); err != nil {
			logger.WithField("tokenizer", "kagome_ja").Error(err)
		}
	}
}

// enableTokenization records a tokenization as available. It is idempotent:
// initialization skips its work when the dictionary is already loaded, so an
// append guarded by that check ran once per process and could not put the
// entry back after a test restored the list.
func enableTokenization(name string) {
	if slices.Contains(Tokenizations, name) {
		return
	}
	Tokenizations = append(Tokenizations, name)
}

func init_gse() error {
	gseLock.Lock()
	defer gseLock.Unlock()
	if gseTokenizer == nil {
		startTime := time.Now()
		seg, err := gse.New("ja")
		if err != nil {
			return fmt.Errorf("load ja dictionary: %w", err)
		}
		gseTokenizer = &seg
		monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues(models.PropertyTokenizationGse).Observe(time.Since(startTime).Seconds())
	}
	// Outside the guard: the flag says the tokenizer is usable, which is true
	// whenever the dictionary is loaded, not only on the call that loaded it.
	// Setting it inside meant a second init found the segmenter already there,
	// skipped the block, and left the flag off.
	UseGse = true
	enableTokenization(models.PropertyTokenizationGse)
	return nil
}

func init_gse_ch() error {
	gseLock.Lock()
	defer gseLock.Unlock()
	if gseTokenizerCh == nil {
		startTime := time.Now()
		seg, err := gse.New("zh")
		if err != nil {
			return fmt.Errorf("load zh dictionary: %w", err)
		}
		gseTokenizerCh = &seg
		monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues(models.PropertyTokenizationGseCh).Observe(time.Since(startTime).Seconds())
	}
	UseGseCh = true
	enableTokenization(models.PropertyTokenizationGseCh)
	return nil
}

// boundTokenizerMetrics caches one tokenization label's prometheus handles:
// WithLabelValues does a registry lookup per call, too costly per value.
type boundTokenizerMetrics struct {
	duration         prometheus.Observer
	tokenCount       prometheus.Counter
	tokensPerRequest prometheus.Observer
}

// record writes one tokenization's worth of metrics: n tokens produced over dur.
func (m *boundTokenizerMetrics) record(dur time.Duration, n int) {
	m.duration.Observe(dur.Seconds())
	m.tokenCount.Add(float64(n))
	m.tokensPerRequest.Observe(float64(n))
}

var boundMetricsByLabel sync.Map // tokenization label → *boundTokenizerMetrics

func metricsFor(label string) *boundTokenizerMetrics {
	if m, ok := boundMetricsByLabel.Load(label); ok {
		return m.(*boundTokenizerMetrics)
	}
	// LoadOrStore evaluates eagerly, so a losing racer builds a second struct —
	// harmless, both wrap the identical prometheus children
	mon := monitoring.GetMetrics()
	m, _ := boundMetricsByLabel.LoadOrStore(label, &boundTokenizerMetrics{
		duration:         mon.TokenizerDuration.WithLabelValues(label),
		tokenCount:       mon.TokenCount.WithLabelValues(label),
		tokensPerRequest: mon.TokenCountPerRequest.WithLabelValues(label),
	})
	return m.(*boundTokenizerMetrics)
}

// timed runs tokenize on in (appending to dst) and records duration and
// token counts under label — the tokenize functions themselves record
// nothing. The count is the appended delta. Guard early-outs must return
// before timed to stay unrecorded.
func timed(label string, in string, dst []string, tokenize func(string, []string) []string) []string {
	start := time.Now()
	ret := tokenize(in, dst)
	metricsFor(label).record(time.Since(start), len(ret)-len(dst))
	return ret
}

// resolveTokenizer maps a tokenization (and, for kagome, the class's custom
// user-dictionary tokenizer) to its append-style tokenize function, so batch callers pay
// dispatch once. fn == nil means unknown tokenization or unavailable tokenizer;
// throttled reports that ApacTokenizerThrottle must be held around calls.
func resolveTokenizer(tokenization string, class string) (fn func(string, []string) []string, throttled bool) {
	switch tokenization {
	case models.PropertyTokenizationWord:
		return tokenizeWord, false
	case models.PropertyTokenizationLowercase:
		return tokenizeLowercase, false
	case models.PropertyTokenizationWhitespace:
		return tokenizeWhitespace, false
	case models.PropertyTokenizationField:
		return tokenizeField, false
	case models.PropertyTokenizationTrigram:
		return tokenizetrigram, false
	case models.PropertyTokenizationGse:
		if !UseGse {
			return nil, false
		}
		return tokenizeGSE, true
	case models.PropertyTokenizationGseCh:
		if !UseGseCh {
			return nil, false
		}
		return tokenizeGseCh, true
	case models.PropertyTokenizationKagomeKr:
		if custom, ok := customTokenizers.Load(class); ok && custom.(*KagomeTokenizers).Korean != nil {
			return func(in string, dst []string) []string {
				return tokenizeKagome(custom.(*KagomeTokenizers).Korean, kagomeTokenizer.Normal, in, dst)
			}, true
		}
		if tokenizers.Korean == nil {
			return nil, false
		}
		return func(in string, dst []string) []string {
			return tokenizeKagome(tokenizers.Korean, kagomeTokenizer.Normal, in, dst)
		}, true
	case models.PropertyTokenizationKagomeJa:
		if custom, ok := customTokenizers.Load(class); ok && custom.(*KagomeTokenizers).Japanese != nil {
			return func(in string, dst []string) []string {
				return tokenizeKagome(custom.(*KagomeTokenizers).Japanese, kagomeTokenizer.Search, in, dst)
			}, true
		}
		if tokenizers.Japanese == nil {
			return nil, false
		}
		return func(in string, dst []string) []string {
			return tokenizeKagome(tokenizers.Japanese, kagomeTokenizer.Search, in, dst)
		}, true
	default:
		return nil, false
	}
}

// TokenizeForClass tokenizes like [Tokenize], except that the kagome
// tokenizations consult the class's custom user-dictionary tokenizer first.
func TokenizeForClass(tokenization string, in string, class string) []string {
	return tokenizeWithClass(tokenization, in, class)
}

func Tokenize(tokenization string, in string) []string {
	// no class: kagome resolves to the globally initialized tokenizers
	return tokenizeWithClass(tokenization, in, "")
}

func tokenizeWithClass(tokenization string, in string, class string) []string {
	fn, throttled := resolveTokenizer(tokenization, class)
	if fn == nil {
		return []string{}
	}
	if throttled {
		defer acquireThrottleSlot()()
	}
	// empty non-nil seed keeps the public contract of a non-nil result
	return timed(tokenization, in, []string{}, fn)
}

func TokenizeWithWildcardsForClass(tokenization string, in string, class string) []string {
	switch tokenization {
	case models.PropertyTokenizationWord:
		return timed("word_with_wildcards", in, []string{}, tokenizeWordWithWildcards)
	case models.PropertyTokenizationTrigram:
		return timed("trigram_with_wildcards", in, []string{}, tokenizetrigramWithWildcards)
	default:
		return TokenizeForClass(tokenization, in, class)
	}
}

// appendNonEmpty appends terms to dst, dropping the empty and single-space
// entries the gse and kagome tokenizers produce in their library output.
func appendNonEmpty(dst []string, terms []string) []string {
	for _, term := range terms {
		if term == "" || term == " " {
			continue
		}
		dst = append(dst, term)
	}
	return dst
}

// The tokenize functions share one append-style signature: tokens are
// appended to dst and the grown slice returned. Batch callers reuse one
// buffer across values; single-value callers seed dst with an empty slice
// and get a never-nil result. When dst has no capacity, the split-based
// functions return the splitter's slice directly to spare the append copy.

// tokenizeField trims white spaces; the whole trimmed input is the one and
// only token (former DataTypeString/Field)
func tokenizeField(in string, dst []string) []string {
	return append(dst, strings.TrimFunc(in, unicode.IsSpace))
}

// appendFieldsFunc is strings.FieldsFunc with an append-style tail: fields are
// appended to dst instead of a freshly allocated slice, so a batch caller
// reusing one buffer splits without allocating per value. The span pass is
// stdlib's — recording indices before materializing substrings measures
// faster than emitting them inline — with the final make replaced by one Grow.
func appendFieldsFunc(dst []string, in string, isSep func(rune) bool) []string {
	type span struct{ start, end int }
	var spanBuf [32]span
	spans := spanBuf[:0]

	start := -1
	for end, r := range in {
		if isSep(r) {
			if start >= 0 {
				spans = append(spans, span{start, end})
				start = -1
			}
		} else if start < 0 {
			start = end
		}
	}
	if start >= 0 {
		spans = append(spans, span{start, len(in)})
	}

	dst = slices.Grow(dst, len(spans))
	for _, s := range spans {
		dst = append(dst, in[s.start:s.end])
	}
	return dst
}

func isNotAlphanumeric(r rune) bool {
	return !unicode.IsLetter(r) && !unicode.IsNumber(r)
}

// tokenizeWhitespace splits on white spaces, does not alter casing
// (former DataTypeString/Word)
func tokenizeWhitespace(in string, dst []string) []string {
	return appendFieldsFunc(dst, in, unicode.IsSpace)
}

// tokenizeLowercase splits on white spaces and lowercases the words
func tokenizeLowercase(in string, dst []string) []string {
	prev := len(dst)
	dst = tokenizeWhitespace(in, dst)
	lowercase(dst[prev:])
	return dst
}

// tokenizeWord splits on any non-alphanumerical and lowercases the words
// (former DataTypeText/Word)
func tokenizeWord(in string, dst []string) []string {
	prev := len(dst)
	dst = appendFieldsFunc(dst, in, isNotAlphanumeric)
	lowercase(dst[prev:])
	return dst
}

// tokenizetrigram splits on any non-alphanumerical and lowercases the words, joins them together, then groups them into trigrams
func tokenizetrigram(in string, dst []string) []string {
	// Strip whitespace and punctuation from the input string
	inputString := strings.ToLower(strings.Join(strings.FieldsFunc(in, isNotAlphanumeric), ""))
	// Byte offset of each rune start, plus a terminator, so a trigram is the
	// substring inputString[off[i]:off[i+3]] — sliced, not copied, which is
	// what keeps a trigram batch from allocating once per emitted trigram.
	var offBuf [128]int
	offs := offBuf[:0]
	for i := range inputString {
		offs = append(offs, i)
	}
	offs = append(offs, len(inputString))
	for i := 0; i+3 < len(offs); i++ {
		dst = append(dst, inputString[offs[i]:offs[i+3]])
	}
	return dst
}

// tokenizeGSE uses the gse tokenizer to tokenize Japanese
func tokenizeGSE(in string, dst []string) []string {
	gseLock.Lock()
	defer gseLock.Unlock()
	return appendNonEmpty(dst, gseTokenizer.CutAll(in))
}

// tokenizeGseCh uses the gse tokenizer to tokenize Chinese
func tokenizeGseCh(in string, dst []string) []string {
	gseLock.Lock()
	defer gseLock.Unlock()
	return appendNonEmpty(dst, gseTokenizerCh.CutAll(in))
}

func initializeKagomeTokenizerKr(userDict *models.TokenizerUserDictConfig) (*kagomeTokenizer.Tokenizer, error) {
	startTime := time.Now()

	dictInstance := koDict.Dict()
	tokenizer, err := initializeKagomeTokenizer(dictInstance, userDict)
	if err != nil {
		return nil, err
	}
	monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues(models.PropertyTokenizationKagomeKr).Observe(float64(time.Since(startTime).Seconds()))
	return tokenizer, nil
}

func initializeKagomeTokenizerJa(userDict *models.TokenizerUserDictConfig) (*kagomeTokenizer.Tokenizer, error) {
	startTime := time.Now()

	dictInstance := ipa.Dict()
	tokenizer, err := initializeKagomeTokenizer(dictInstance, userDict)
	if err != nil {
		return nil, err
	}
	monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues(models.PropertyTokenizationKagomeJa).Observe(float64(time.Since(startTime).Seconds()))
	return tokenizer, nil
}

func initializeKagomeTokenizer(dictInstance *dict.Dict, userDict *models.TokenizerUserDictConfig) (*kagomeTokenizer.Tokenizer, error) {
	options := []kagomeTokenizer.Option{
		kagomeTokenizer.OmitBosEos(),
	}

	if userDict != nil {
		dict, err := NewUserDictFromModel(userDict)
		if err != nil {
			return nil, err
		}
		if dict != nil {
			options = append(options, kagomeTokenizer.UserDict(dict))
		}
	}
	tokenizer, err := kagomeTokenizer.New(dictInstance, options...)
	if err != nil {
		return nil, err
	}
	return tokenizer, nil
}

func tokenizeKagome(tokenizer *kagomeTokenizer.Tokenizer, mode kagomeTokenizer.TokenizeMode, in string, dst []string) []string {
	if tokenizer == nil {
		return dst
	}
	kagomeTokens := tokenizer.Analyze(in, mode)
	for _, token := range kagomeTokens {
		if extra := token.UserExtra(); extra != nil {
			dst = appendNonEmpty(dst, extra.Tokens)
		} else if token.Surface != "" && token.Surface != " " {
			dst = append(dst, token.Surface)
		}
	}
	return dst
}

// tokenizeWordWithWildcards splits on any non-alphanumerical except wildcard-symbols and
// lowercases the words
func tokenizeWordWithWildcards(in string, dst []string) []string {
	prev := len(dst)
	dst = appendFieldsFunc(dst, in, func(r rune) bool {
		return isNotAlphanumeric(r) && r != '?' && r != '*'
	})
	lowercase(dst[prev:])
	return dst
}

// tokenizetrigramWithWildcards splits on any non-alphanumerical and lowercases the words, applies any wildcards, then joins them together, then groups them into trigrams
// this is unlikely to be useful, but is included for completeness
func tokenizetrigramWithWildcards(in string, dst []string) []string {
	inputString := strings.Join(tokenizeWordWithWildcards(in, nil), "")
	for i := 0; i < len(inputString)-2; i++ {
		dst = append(dst, inputString[i:i+3])
	}
	return dst
}

func lowercase(terms []string) {
	for i := range terms {
		terms[i] = strings.ToLower(terms[i])
	}
}

func TokenizeAndCountDuplicatesForClass(tokenization string, in string, class string) ([]string, []int) {
	counts := map[string]int{}
	for _, term := range TokenizeForClass(tokenization, in, class) {
		counts[term]++
	}

	unique := make([]string, len(counts))
	boosts := make([]int, len(counts))

	i := 0
	for term, boost := range counts {
		unique[i] = term
		boosts[i] = boost
		i++
	}

	return unique, boosts
}

// SaveOptionalTokenizerState captures the process-global state that
// InitOptionalTokenizers mutates and returns a function restoring it.
//
// t.Setenv puts the environment back but not the state derived from it: the
// UseGse flags, the Kagome tokenizers and the Tokenizations list all outlive
// the test that enabled them, so a later test asking whether an optional
// tokenizer is disabled sees it enabled. Tests that call
// InitOptionalTokenizers should register this with t.Cleanup.
func SaveOptionalTokenizerState() func() {
	useGse, useGseCh := UseGse, UseGseCh
	savedKagome := tokenizers
	savedTokenizations := append([]string(nil), Tokenizations...)

	return func() {
		UseGse, UseGseCh = useGse, useGseCh
		tokenizers = savedKagome
		Tokenizations = savedTokenizations
	}
}
