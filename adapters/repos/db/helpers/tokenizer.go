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
	"log"
	"os"
	"strings"
	"sync"
	"unicode"

	"github.com/go-ego/gse"
	koDict "github.com/ikawaha/kagome-dict-ko"
	kagomeDict "github.com/ikawaha/kagome-dict/dict"
	kagomeTokenizer "github.com/ikawaha/kagome/v2/tokenizer"
	"github.com/weaviate/weaviate/entities/models"
)

var (
	gseTokenizer     *gse.Segmenter
	gseTokenizerLock = &sync.Mutex{}
	UseGse           = false
)

var Tokenizations []string = []string{
	models.PropertyTokenizationWord,
	models.PropertyTokenizationLowercase,
	models.PropertyTokenizationWhitespace,
	models.PropertyTokenizationField,
	models.PropertyTokenizationTrigram,
	models.PropertyTokenizationGse,
	models.PropertyTokenizationKagomeKr,
}

func init() {
	init_gse()
}

func init_gse() {
	if os.Getenv("USE_GSE") == "true" {
		UseGse = true
	}
	if UseGse {
		gseTokenizerLock.Lock()
		defer gseTokenizerLock.Unlock()
		if gseTokenizer == nil {
			seg, err := gse.New("ja")
			if err != nil {
				return //[]string{}
			}
			gseTokenizer = &seg
		}
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
		return tokenizeGSE(in)
	case models.PropertyTokenizationKagomeKr:
		return tokenizeKagomeKr(in)
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
		return tokenizeGSE(in)
	case models.PropertyTokenizationKagomeKr:
		return tokenizeKagomeKr(in)
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

// tokenizeGSE uses the gse tokenizer to tokenise Chinese and Japanese
func tokenizeGSE(in string) []string {
	if !UseGse {
		return []string{}
	}
	gseTokenizerLock.Lock()
	defer gseTokenizerLock.Unlock()
	terms := gseTokenizer.CutAll(in)

	terms = removeEmptyStrings(terms)

	alpha := tokenizeWord(in)
	return append(terms, alpha...)
}

type KagomeTokenizer struct {
	tokenizer *kagomeTokenizer.Tokenizer
	mutex     sync.RWMutex
}

var koreanTokenizer KagomeTokenizer

func initializeKagomeTokenizer(dictInstance *kagomeDict.Dict, tokenizer *KagomeTokenizer, language string) error {
	tokenizer.mutex.Lock()
	defer tokenizer.mutex.Unlock()

	if tokenizer.tokenizer != nil {
		return nil // Already initialized
	}

	newTokenizer, err := kagomeTokenizer.New(dictInstance)
	if err != nil {
		log.Printf("failed to create a tokenizer using: %v; error: %v", language, err)
		return err
	}

	tokenizer.tokenizer = newTokenizer
	log.Printf("successfully created a tokenizer using: %v", language)
	return nil
}

func InitializeKagomeTokenizerKr() error {
	return initializeKagomeTokenizer(koDict.Dict(), &koreanTokenizer, "Korean")
}

func tokenizeWithKagome(in string, tokenizer *KagomeTokenizer) []string {
	tokenizer.mutex.Lock()
	defer tokenizer.mutex.Unlock()

	if tokenizer.tokenizer == nil {
		return []string{}
	}

	kagomeTokens := tokenizer.tokenizer.Tokenize(in)
	terms := make([]string, 0, len(kagomeTokens))

	for _, token := range kagomeTokens {
		if token.Surface != "EOS" && token.Surface != "BOS" {
			terms = append(terms, token.Surface)
		}
	}

	return removeEmptyStrings(terms)
}

var initKoreanTokenizerOnce sync.Once

func tokenizeKagomeKr(in string) []string {
	var initErr error
	initKoreanTokenizerOnce.Do(func() {
		initErr = InitializeKagomeTokenizerKr()
	})
	if initErr != nil {
		log.Printf("Korean tokenizer not available: %v", initErr)
		return []string{}
	}
	return tokenizeWithKagome(in, &koreanTokenizer)
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
