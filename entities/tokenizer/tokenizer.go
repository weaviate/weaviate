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

// throttleCapacity computes the ApacTokenizerThrottle capacity from the
// TOKENIZER_CONCURRENCY_COUNT env value. Non-numeric values and values below
// 1 are rejected with a warning and fall back to runtime.GOMAXPROCS(0), like
// an unset variable: a capacity-0 channel would deadlock the first
// tokenization (an acquire with no holder to ever release it), and make
// panics on a negative capacity.
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
			Tokenizations = append(Tokenizations, models.PropertyTokenizationKagomeKr)
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
			Tokenizations = append(Tokenizations, models.PropertyTokenizationKagomeJa)
			return nil
		}(); err != nil {
			logger.WithField("tokenizer", "kagome_ja").Error(err)
		}
	}
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
		UseGse = true
		Tokenizations = append(Tokenizations, models.PropertyTokenizationGse)
		monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues(models.PropertyTokenizationGse).Observe(time.Since(startTime).Seconds())
	}
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
		UseGseCh = true
		Tokenizations = append(Tokenizations, models.PropertyTokenizationGseCh)
		monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues(models.PropertyTokenizationGseCh).Observe(time.Since(startTime).Seconds())
	}
	return nil
}

// boundTokenizerMetrics holds the prometheus handles for one tokenization
// label, resolved once. WithLabelValues performs a registry lookup (label
// hashing plus a locked map access) on every call; hot paths tokenize once
// per value, so the handles are bound once per label and cached instead.
type boundTokenizerMetrics struct {
	duration         prometheus.Observer
	tokenCount       prometheus.Counter
	tokensPerRequest prometheus.Observer
}

var boundMetricsByLabel sync.Map // tokenization label → *boundTokenizerMetrics

func metricsFor(label string) *boundTokenizerMetrics {
	if m, ok := boundMetricsByLabel.Load(label); ok {
		return m.(*boundTokenizerMetrics)
	}
	// Cold path: first call for this label (or a concurrent first-call race).
	// LoadOrStore evaluates its value argument eagerly, so a losing racer
	// builds a second struct — harmless: WithLabelValues is idempotent (both
	// structs wrap the identical prometheus children), the loser is garbage,
	// and this runs at most a handful of times per process.
	mon := monitoring.GetMetrics()
	m, _ := boundMetricsByLabel.LoadOrStore(label, &boundTokenizerMetrics{
		duration:         mon.TokenizerDuration.WithLabelValues(label),
		tokenCount:       mon.TokenCount.WithLabelValues(label),
		tokensPerRequest: mon.TokenCountPerRequest.WithLabelValues(label),
	})
	return m.(*boundTokenizerMetrics)
}

// timed runs tokenize on in and records duration and token counts under
// label. All tokenization metrics are recorded here, at the dispatch level —
// the tokenizeXxx functions contain pure logic. Callers that bypass a
// tokenizer's dispatch guard (disabled tokenizer, nil kagome instance) must
// return before timed so early-outs stay unrecorded, as they always were.
func timed(label string, in string, tokenize func(string) []string) []string {
	start := time.Now()
	ret := tokenize(in)
	m := metricsFor(label)
	m.duration.Observe(time.Since(start).Seconds())
	m.tokenCount.Add(float64(len(ret)))
	m.tokensPerRequest.Observe(float64(len(ret)))
	return ret
}

// TokenizeForClass tokenizes like [Tokenize], except that the kagome
// tokenizations consult the class's custom user-dictionary tokenizer first.
func TokenizeForClass(tokenization string, in string, class string) []string {
	switch tokenization {
	case models.PropertyTokenizationKagomeKr:
		if custom, ok := customTokenizers.Load(class); ok && custom.(*KagomeTokenizers).Korean != nil {
			ApacTokenizerThrottle <- struct{}{}
			defer func() { <-ApacTokenizerThrottle }()
			return timed(tokenization, in, func(in string) []string {
				return tokenizeKagome(custom.(*KagomeTokenizers).Korean, kagomeTokenizer.Normal, in)
			})
		}
	case models.PropertyTokenizationKagomeJa:
		if custom, ok := customTokenizers.Load(class); ok && custom.(*KagomeTokenizers).Japanese != nil {
			ApacTokenizerThrottle <- struct{}{}
			defer func() { <-ApacTokenizerThrottle }()
			return timed(tokenization, in, func(in string) []string {
				return tokenizeKagome(custom.(*KagomeTokenizers).Japanese, kagomeTokenizer.Search, in)
			})
		}
	}
	return Tokenize(tokenization, in)
}

func Tokenize(tokenization string, in string) []string {
	switch tokenization {
	case models.PropertyTokenizationWord:
		return timed(tokenization, in, tokenizeWord)
	case models.PropertyTokenizationLowercase:
		return timed(tokenization, in, tokenizeLowercase)
	case models.PropertyTokenizationWhitespace:
		return timed(tokenization, in, tokenizeWhitespace)
	case models.PropertyTokenizationField:
		return timed(tokenization, in, tokenizeField)
	case models.PropertyTokenizationTrigram:
		return timed(tokenization, in, tokenizetrigram)
	case models.PropertyTokenizationGse:
		if !UseGse {
			return []string{}
		}
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return timed(tokenization, in, tokenizeGSE)
	case models.PropertyTokenizationGseCh:
		if !UseGseCh {
			return []string{}
		}
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return timed(tokenization, in, tokenizeGseCh)
	case models.PropertyTokenizationKagomeKr:
		if tokenizers.Korean == nil {
			return []string{}
		}
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return timed(tokenization, in, func(in string) []string {
			return tokenizeKagome(tokenizers.Korean, kagomeTokenizer.Normal, in)
		})
	case models.PropertyTokenizationKagomeJa:
		if tokenizers.Japanese == nil {
			return []string{}
		}
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return timed(tokenization, in, func(in string) []string {
			return tokenizeKagome(tokenizers.Japanese, kagomeTokenizer.Search, in)
		})
	default:
		return []string{}
	}
}

func TokenizeWithWildcardsForClass(tokenization string, in string, class string) []string {
	switch tokenization {
	case models.PropertyTokenizationWord:
		return timed("word_with_wildcards", in, tokenizeWordWithWildcards)
	case models.PropertyTokenizationTrigram:
		return timed("trigram_with_wildcards", in, tokenizetrigramWithWildcards)
	default:
		return TokenizeForClass(tokenization, in, class)
	}
}

func removeEmptyStrings(terms []string) []string {
	for i := 0; i < len(terms); i++ {
		if terms[i] == "" || terms[i] == " " {
			terms = append(terms[:i], terms[i+1:]...)
			i--
		}
	}
	return terms
}

// tokenizeField trims white spaces
// (former DataTypeString/Field)
func tokenizeField(in string) []string {
	return []string{strings.TrimFunc(in, unicode.IsSpace)}
}

// tokenizeWhitespace splits on white spaces, does not alter casing
// (former DataTypeString/Word)
func tokenizeWhitespace(in string) []string {
	return strings.FieldsFunc(in, unicode.IsSpace)
}

// tokenizeLowercase splits on white spaces and lowercases the words
func tokenizeLowercase(in string) []string {
	return lowercase(tokenizeWhitespace(in))
}

// tokenizeWord splits on any non-alphanumerical and lowercases the words
// (former DataTypeText/Word)
func tokenizeWord(in string) []string {
	terms := strings.FieldsFunc(in, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	return lowercase(terms)
}

// tokenizetrigram splits on any non-alphanumerical and lowercases the words, joins them together, then groups them into trigrams
func tokenizetrigram(in string) []string {
	// Strip whitespace and punctuation from the input string
	inputString := strings.ToLower(strings.Join(strings.FieldsFunc(in, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}), ""))
	runes := []rune(inputString)
	var trirunes [][]rune
	for i := 0; i < len(runes)-2; i++ {
		trirunes = append(trirunes, runes[i:i+3])
	}

	var trigrams []string
	for _, trirune := range trirunes {
		trigrams = append(trigrams, string(trirune))
	}
	return trigrams
}

// tokenizeGSE uses the gse tokenizer to tokenize Japanese
func tokenizeGSE(in string) []string {
	if !UseGse {
		return []string{}
	}
	gseLock.Lock()
	defer gseLock.Unlock()
	return removeEmptyStrings(gseTokenizer.CutAll(in))
}

// tokenizeGseCh uses the gse tokenizer to tokenize Chinese
func tokenizeGseCh(in string) []string {
	if !UseGseCh {
		return []string{}
	}
	gseLock.Lock()
	defer gseLock.Unlock()
	return removeEmptyStrings(gseTokenizerCh.CutAll(in))
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

func tokenizeKagome(tokenizer *kagomeTokenizer.Tokenizer, mode kagomeTokenizer.TokenizeMode, in string) []string {
	if tokenizer == nil {
		return []string{}
	}
	kagomeTokens := tokenizer.Analyze(in, mode)
	var terms []string
	for _, token := range kagomeTokens {
		if extra := token.UserExtra(); extra != nil {
			terms = append(terms, extra.Tokens...)
		} else {
			terms = append(terms, token.Surface)
		}
	}

	return removeEmptyStrings(terms)
}

// tokenizeWordWithWildcards splits on any non-alphanumerical except wildcard-symbols and
// lowercases the words
func tokenizeWordWithWildcards(in string) []string {
	terms := strings.FieldsFunc(in, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '?' && r != '*'
	})
	return lowercase(terms)
}

// tokenizetrigramWithWildcards splits on any non-alphanumerical and lowercases the words, applies any wildcards, then joins them together, then groups them into trigrams
// this is unlikely to be useful, but is included for completeness
func tokenizetrigramWithWildcards(in string) []string {
	terms := tokenizeWordWithWildcards(in)
	inputString := strings.Join(terms, "")
	var trigrams []string
	for i := 0; i < len(inputString)-2; i++ {
		trigrams = append(trigrams, inputString[i:i+3])
	}
	return trigrams
}

func lowercase(terms []string) []string {
	for i := range terms {
		terms[i] = strings.ToLower(terms[i])
	}
	return terms
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
