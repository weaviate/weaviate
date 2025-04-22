//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helpers

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	entcfg "github.com/weaviate/weaviate/entities/config"

	"github.com/go-ego/gse"
	koDict "github.com/ikawaha/kagome-dict-ko"
	"github.com/ikawaha/kagome-dict/ipa"
	kagomeTokenizer "github.com/ikawaha/kagome/v2/tokenizer"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

var (
	gseTokenizer    *gse.Segmenter  // Japanese
	gseTokenizerCh  *gse.Segmenter  // Chinese
	gseLock         = &sync.Mutex{} // Lock for gse
	UseGse          = false         // Load Japanese dictionary and prepare tokenizer
	UseGseCh        = false         // Load Chinese dictionary and prepare tokenizer
	KagomeKrEnabled = false         // Load Korean dictionary and prepare tokenizer
	KagomeJaEnabled = false         // Load Japanese dictionary and prepare tokenizer
	// The Tokenizer Libraries can consume a lot of memory, so we limit the number of parallel tokenizers
	ApacTokenizerThrottle = chan struct{}(nil) // Throttle for tokenizers
	tokenizers            KagomeTokenizers     // Tokenizers for Korean and Japanese
	kagomeInitLock        sync.Mutex           // Lock for kagome initialization
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
	numParallel := runtime.GOMAXPROCS(0)
	numParallelStr := os.Getenv("TOKENIZER_CONCURRENCY_COUNT")
	if numParallelStr != "" {
		x, err := strconv.Atoi(numParallelStr)
		if err == nil {
			numParallel = x
		}
	}
	ApacTokenizerThrottle = make(chan struct{}, numParallel)
	if entcfg.Enabled(os.Getenv("USE_GSE")) || entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_GSE")) {
		UseGse = true
		Tokenizations = append(Tokenizations, models.PropertyTokenizationGse)
		init_gse()
	}
	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_GSE_CH")) {
		Tokenizations = append(Tokenizations, models.PropertyTokenizationGseCh)
		UseGseCh = true
		init_gse_ch()
	}
	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_KR")) {
		Tokenizations = append(Tokenizations, models.PropertyTokenizationKagomeKr)
	}
	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_JA")) {
		Tokenizations = append(Tokenizations, models.PropertyTokenizationKagomeJa)
	}
	_ = initializeKagomeTokenizerKr()
	_ = initializeKagomeTokenizerJa()
}

func init_gse() {
	gseLock.Lock()
	defer gseLock.Unlock()
	if gseTokenizer == nil {
		startTime := time.Now()
		seg, err := gse.New("ja")
		if err != nil {
			return
		}
		gseTokenizer = &seg
		monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues("gse").Observe(time.Since(startTime).Seconds())
	}
}

func init_gse_ch() {
	gseLock.Lock()
	defer gseLock.Unlock()
	if gseTokenizerCh == nil {
		startTime := time.Now()
		seg, err := gse.New("zh")
		if err != nil {
			return
		}
		gseTokenizerCh = &seg
		monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues("gse").Observe(time.Since(startTime).Seconds())
	}
}

func Tokenize(tokenization string, in string) []string {
	switch tokenization {
	case models.PropertyTokenizationWord:
		return tokenizeWord(in)
	case models.PropertyTokenizationLowercase:
		return tokenizeLowercase(in)
	case models.PropertyTokenizationWhitespace:
		return tokenizeWhitespace(in)
	case models.PropertyTokenizationField:
		return tokenizeField(in)
	case models.PropertyTokenizationTrigram:
		return tokenizetrigram(in)
	case models.PropertyTokenizationGse:
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return tokenizeGSE(in)
	case models.PropertyTokenizationGseCh:
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return tokenizeGseCh(in)
	case models.PropertyTokenizationKagomeKr:
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return tokenizeKagomeKr(in)
	case models.PropertyTokenizationKagomeJa:
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return tokenizeKagomeJa(in)
	default:
		return []string{}
	}
}

func TokenizeWithWildcards(tokenization string, in string) []string {
	switch tokenization {
	case models.PropertyTokenizationWord:
		return tokenizeWordWithWildcards(in)
	case models.PropertyTokenizationLowercase:
		return tokenizeLowercase(in)
	case models.PropertyTokenizationWhitespace:
		return tokenizeWhitespace(in)
	case models.PropertyTokenizationField:
		return tokenizeField(in)
	case models.PropertyTokenizationTrigram:
		return tokenizetrigramWithWildcards(in)
	case models.PropertyTokenizationGse:
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return tokenizeGSE(in)
	case models.PropertyTokenizationGseCh:
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return tokenizeGseCh(in)
	case models.PropertyTokenizationKagomeKr:
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return tokenizeKagomeKr(in)
	case models.PropertyTokenizationKagomeJa:
		ApacTokenizerThrottle <- struct{}{}
		defer func() { <-ApacTokenizerThrottle }()
		return tokenizeKagomeJa(in)
	default:
		return []string{}
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
	startTime := time.Now()
	ret := []string{strings.TrimFunc(in, unicode.IsSpace)}
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("field").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("field").Add(float64(len(ret)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("field").Observe(float64(len(ret)))
	return ret
}

// tokenizeWhitespace splits on white spaces, does not alter casing
// (former DataTypeString/Word)
func tokenizeWhitespace(in string) []string {
	startTime := time.Now()
	ret := strings.FieldsFunc(in, unicode.IsSpace)
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("whitespace").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("whitespace").Add(float64(len(ret)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("whitespace").Observe(float64(len(ret)))
	return ret
}

// tokenizeLowercase splits on white spaces and lowercases the words
func tokenizeLowercase(in string) []string {
	startTime := time.Now()
	terms := tokenizeWhitespace(in)
	ret := lowercase(terms)
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("lowercase").Observe(float64(time.Since(startTime).Seconds()))
	return ret
}

// tokenizeWord splits on any non-alphanumerical and lowercases the words
// (former DataTypeText/Word)
func tokenizeWord(in string) []string {
	startTime := time.Now()
	terms := strings.FieldsFunc(in, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	ret := lowercase(terms)
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("word").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("word").Add(float64(len(ret)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("word").Observe(float64(len(ret)))
	return ret
}

// tokenizetrigram splits on any non-alphanumerical and lowercases the words, joins them together, then groups them into trigrams
func tokenizetrigram(in string) []string {
	startTime := time.Now()
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
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("trigram").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("trigram").Add(float64(len(trigrams)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("trigram").Observe(float64(len(trigrams)))
	return trigrams
}

// tokenizeGSE uses the gse tokenizer to tokenise Japanese
func tokenizeGSE(in string) []string {
	if !UseGse {
		return []string{}
	}
	startTime := time.Now()
	gseLock.Lock()
	defer gseLock.Unlock()
	terms := gseTokenizer.CutAll(in)

	ret := removeEmptyStrings(terms)

	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("gse").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("gse").Add(float64(len(ret)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("gse").Observe(float64(len(ret)))
	return ret
}

// tokenizeGSE uses the gse tokenizer to tokenise Chinese
func tokenizeGseCh(in string) []string {
	if !UseGseCh {
		return []string{}
	}
	gseLock.Lock()
	defer gseLock.Unlock()
	startTime := time.Now()
	terms := gseTokenizerCh.CutAll(in)
	ret := removeEmptyStrings(terms)

	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("gse").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("gse").Add(float64(len(ret)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("gse").Observe(float64(len(ret)))
	return ret
}

func initializeKagomeTokenizerKr() error {
	// Acquire lock to prevent initialization race
	kagomeInitLock.Lock()
	defer kagomeInitLock.Unlock()

	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_KR")) {
		if tokenizers.Korean != nil {
			return nil
		}
		startTime := time.Now()

		dictInstance := koDict.Dict()
		tokenizer, err := kagomeTokenizer.New(dictInstance)
		if err != nil {
			return err
		}

		tokenizers.Korean = tokenizer
		KagomeKrEnabled = true
		monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues("kagome_kr").Observe(float64(time.Since(startTime).Seconds()))
		return nil
	}

	return nil
}

func tokenizeKagomeKr(in string) []string {
	tokenizer := tokenizers.Korean
	if tokenizer == nil || !KagomeKrEnabled {
		return []string{}
	}
	startTime := time.Now()

	kagomeTokens := tokenizer.Tokenize(in)
	terms := make([]string, 0, len(kagomeTokens))

	for _, token := range kagomeTokens {
		if token.Surface != "EOS" && token.Surface != "BOS" {
			terms = append(terms, token.Surface)
		}
	}

	ret := removeEmptyStrings(terms)
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("kagome_kr").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("kagome_kr").Add(float64(len(ret)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("kagome_kr").Observe(float64(len(ret)))
	return ret
}

func initializeKagomeTokenizerJa() error {
	// Acquire lock to prevent initialization race
	kagomeInitLock.Lock()
	defer kagomeInitLock.Unlock()

	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_JA")) {
		if tokenizers.Japanese != nil {
			return nil
		}
		startTime := time.Now()
		dictInstance := ipa.Dict()
		tokenizer, err := kagomeTokenizer.New(dictInstance)
		if err != nil {
			return err
		}

		tokenizers.Japanese = tokenizer
		KagomeJaEnabled = true
		monitoring.GetMetrics().TokenizerInitializeDuration.WithLabelValues("kagome_ja").Observe(float64(time.Since(startTime).Seconds()))
		return nil
	}

	return nil
}

func tokenizeKagomeJa(in string) []string {
	tokenizer := tokenizers.Japanese
	if tokenizer == nil || !KagomeJaEnabled {
		return []string{}
	}

	startTime := time.Now()
	kagomeTokens := tokenizer.Analyze(in, kagomeTokenizer.Search)
	terms := make([]string, 0, len(kagomeTokens))

	for _, token := range kagomeTokens {
		if token.Surface != "EOS" && token.Surface != "BOS" {
			terms = append(terms, strings.ToLower(token.Surface))
		}
	}

	ret := removeEmptyStrings(terms)
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("kagome_ja").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("kagome_ja").Add(float64(len(ret)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("kagome_ja").Observe(float64(len(ret)))
	return ret
}

// tokenizeWordWithWildcards splits on any non-alphanumerical except wildcard-symbols and
// lowercases the words
func tokenizeWordWithWildcards(in string) []string {
	startTime := time.Now()
	terms := strings.FieldsFunc(in, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '?' && r != '*'
	})
	ret := lowercase(terms)
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("word_with_wildcards").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("word_with_wildcards").Add(float64(len(ret)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("word_with_wildcards").Observe(float64(len(ret)))
	return ret
}

// tokenizetrigramWithWildcards splits on any non-alphanumerical and lowercases the words, applies any wildcards, then joins them together, then groups them into trigrams
// this is unlikely to be useful, but is included for completeness
func tokenizetrigramWithWildcards(in string) []string {
	startTime := time.Now()
	terms := tokenizeWordWithWildcards(in)
	inputString := strings.Join(terms, "")
	var trigrams []string
	for i := 0; i < len(inputString)-2; i++ {
		trigrams = append(trigrams, inputString[i:i+3])
	}
	monitoring.GetMetrics().TokenizerDuration.WithLabelValues("trigram_with_wildcards").Observe(float64(time.Since(startTime).Seconds()))
	monitoring.GetMetrics().TokenCount.WithLabelValues("trigram_with_wildcards").Add(float64(len(trigrams)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("trigram_with_wildcards").Observe(float64(len(trigrams)))
	return trigrams
}

func lowercase(terms []string) []string {
	for i := range terms {
		terms[i] = strings.ToLower(terms[i])
	}
	monitoring.GetMetrics().TokenCount.WithLabelValues("lowercase").Add(float64(len(terms)))
	monitoring.GetMetrics().TokenCountPerRequest.WithLabelValues("lowercase").Observe(float64(len(terms)))
	return terms
}

func TokenizeAndCountDuplicates(tokenization string, in string) ([]string, []int) {
	counts := map[string]int{}
	for _, term := range Tokenize(tokenization, in) {
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
