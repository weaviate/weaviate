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
	"unicode"

	entcfg "github.com/weaviate/weaviate/entities/config"

	"github.com/go-ego/gse"
	koDict "github.com/ikawaha/kagome-dict-ko"
	"github.com/ikawaha/kagome-dict/ipa"
	kagomeTokenizer "github.com/ikawaha/kagome/v2/tokenizer"
	"github.com/weaviate/weaviate/entities/models"
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
		if err != nil {
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
	if entcfg.Enabled(os.Getenv("USE_GSE")) || entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_GSE")) {
		UseGse = true
	}
	if UseGse {
		gseLock.Lock()
		defer gseLock.Unlock()
		if gseTokenizer == nil {
			seg, err := gse.New("ja")
			if err != nil {
				return //[]string{}
			}
			gseTokenizer = &seg
		}
	}
}

func init_gse_ch() {
	gseLock.Lock()
	defer gseLock.Unlock()
	if gseTokenizerCh == nil {
		seg, err := gse.New("zh")
		if err != nil {
			return
		}
		gseTokenizerCh = &seg
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
	return []string{strings.TrimFunc(in, unicode.IsSpace)}
}

// tokenizeWhitespace splits on white spaces, does not alter casing
// (former DataTypeString/Word)
func tokenizeWhitespace(in string) []string {
	return strings.FieldsFunc(in, unicode.IsSpace)
}

// tokenizeLowercase splits on white spaces and lowercases the words
func tokenizeLowercase(in string) []string {
	terms := tokenizeWhitespace(in)
	return lowercase(terms)
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

// tokenizeGSE uses the gse tokenizer to tokenise Japanese
func tokenizeGSE(in string) []string {
	if !UseGse {
		return []string{}
	}
	gseLock.Lock()
	defer gseLock.Unlock()
	terms := gseTokenizer.CutAll(in)

	terms = removeEmptyStrings(terms)

	return terms
}

// tokenizeGSE uses the gse tokenizer to tokenise Chinese
func tokenizeGseCh(in string) []string {
	if !UseGseCh {
		return []string{}
	}
	gseLock.Lock()
	defer gseLock.Unlock()
	terms := gseTokenizerCh.CutAll(in)
	terms = removeEmptyStrings(terms)

	return terms
}

func initializeKagomeTokenizerKr() error {
	// Acquire lock to prevent initialization race
	kagomeInitLock.Lock()
	defer kagomeInitLock.Unlock()

	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_KR")) {
		if tokenizers.Korean != nil {
			return nil
		}

		dictInstance := koDict.Dict()
		tokenizer, err := kagomeTokenizer.New(dictInstance)
		if err != nil {
			return err
		}

		tokenizers.Korean = tokenizer
		KagomeKrEnabled = true
		return nil
	}

	return nil
}

func tokenizeKagomeKr(in string) []string {
	tokenizer := tokenizers.Korean
	if tokenizer == nil || !KagomeKrEnabled {
		return []string{}
	}

	kagomeTokens := tokenizer.Tokenize(in)
	terms := make([]string, 0, len(kagomeTokens))

	for _, token := range kagomeTokens {
		if token.Surface != "EOS" && token.Surface != "BOS" {
			terms = append(terms, token.Surface)
		}
	}

	return removeEmptyStrings(terms)
}

func initializeKagomeTokenizerJa() error {
	// Acquire lock to prevent initialization race
	kagomeInitLock.Lock()
	defer kagomeInitLock.Unlock()

	if entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_JA")) {
		if tokenizers.Japanese != nil {
			return nil
		}

		dictInstance := ipa.Dict()
		tokenizer, err := kagomeTokenizer.New(dictInstance)
		if err != nil {
			return err
		}

		tokenizers.Japanese = tokenizer
		KagomeJaEnabled = true
		return nil
	}

	return nil
}

func tokenizeKagomeJa(in string) []string {
	tokenizer := tokenizers.Japanese
	if tokenizer == nil || !KagomeJaEnabled {
		return []string{}
	}

	kagomeTokens := tokenizer.Analyze(in, kagomeTokenizer.Search)
	terms := make([]string, 0, len(kagomeTokens))

	for _, token := range kagomeTokens {
		if token.Surface != "EOS" && token.Surface != "BOS" {
			terms = append(terms, strings.ToLower(token.Surface))
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
