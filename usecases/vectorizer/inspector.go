package vectorizer

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/semi-technologies/weaviate/entities/models"
)

type inspectorClient interface {
	VectorForWord(ctx context.Context, word string) ([]float32, error)
	VectorForCorpi(ctx context.Context, words []string) ([]float32, error)
	NearestWordsByVector(ctx context.Context, vector []float32, n int, k int) ([]string, []float32, error)
	IsWordPresent(ctx context.Context, word string) (bool, error)
}

type Inspector struct {
	client inspectorClient
}

func NewInspector(client inspectorClient) *Inspector {
	return &Inspector{client: client}
}

func (i *Inspector) GetWords(ctx context.Context, words string) (*models.C11yWordsResponse, error) {
	wordArray, err := i.validateAndSplit(words)
	if err != nil {
		return nil, err
	}

	concatWord, err := i.concatWord(ctx, words, wordArray)
	if err != nil {
		return nil, err
	}

	individualWords, err := i.individualWords(ctx, wordArray)
	if err != nil {
		return nil, err
	}

	return &models.C11yWordsResponse{
		ConcatenatedWord: concatWord,
		IndividualWords:  individualWords,
	}, nil
}

func (i *Inspector) validateAndSplit(words string) ([]string, error) {
	// set first character to lowercase
	firstChar := []rune(words)
	firstChar[0] = unicode.ToLower(firstChar[0])
	words = string(firstChar)

	if ok, _ := regexp.MatchString("^[a-zA-Z]+$", words); !ok {
		return nil, fmt.Errorf("invalid word input: words must match ^[a-zA-Z]+$")
	}

	return split(words), nil
}

func (i *Inspector) concatWord(ctx context.Context, words string,
	wordArray []string) (*models.C11yWordsResponseConcatenatedWord, error) {
	if len(wordArray) < 2 {
		// only build a concat response if we have more than a single word
		return nil, nil
	}

	// join the words into a single corpus. While the contextionary also supports
	// building a centroid from multiple corpi (thus []string for Corpi, an
	// occurrence-based weighing can only happen within a corpus. It is thus - by
	// far - preferable in this case, to concat the words into one corpus, rather
	// than treating each word as its own.
	corpus := strings.Join(wordArray, " ")
	vector, err := i.client.VectorForCorpi(ctx, []string{corpus})
	if err != nil {
		return nil, err
	}

	nearestNeighbors, err := i.nearestNeighbors(ctx, vector)
	if err != nil {
		return nil, err
	}

	return &models.C11yWordsResponseConcatenatedWord{
		ConcatenatedWord:             words,
		SingleWords:                  wordArray,
		ConcatenatedVector:           vector,
		ConcatenatedNearestNeighbors: nearestNeighbors,
	}, nil
}

func (i *Inspector) nearestNeighbors(ctx context.Context,
	vector []float32) ([]*models.C11yNearestNeighborsItems0, error) {
	// relate words of centroid
	words, dists, err := i.client.NearestWordsByVector(ctx, vector, 12, 32)
	if err != nil {
		return nil, err
	}

	nearestNeighbors := []*models.C11yNearestNeighborsItems0{}

	// loop over NN Idx' and append to the return object
	for i, word := range words {
		item := models.C11yNearestNeighborsItems0{
			Word:     word,
			Distance: dists[i],
		}

		nearestNeighbors = append(nearestNeighbors, &item)
	}

	return nearestNeighbors, nil
}

func (i *Inspector) individualWords(ctx context.Context,
	wordArray []string) ([]*models.C11yWordsResponseIndividualWordsItems0, error) {
	var res []*models.C11yWordsResponseIndividualWordsItems0

	for _, word := range wordArray {
		iw, err := i.individualWord(ctx, word)
		if err != nil {
			return nil, fmt.Errorf("word '%s': %v", word, err)
		}

		res = append(res, iw)
	}

	return res, nil
}

func (i *Inspector) individualWord(ctx context.Context,
	word string) (*models.C11yWordsResponseIndividualWordsItems0, error) {

	ok, err := i.client.IsWordPresent(ctx, word)
	if err != nil {
		return nil, fmt.Errorf("could not check word presence:  %v", err)
	}

	if !ok {
		return i.individualWordNotPresent(word), nil
	}

	return i.individualWordPresent(ctx, word)
}

func (i *Inspector) individualWordNotPresent(word string) *models.C11yWordsResponseIndividualWordsItems0 {
	return &models.C11yWordsResponseIndividualWordsItems0{
		Word:   word,
		InC11y: false,
	}
}

func (i *Inspector) individualWordPresent(ctx context.Context,
	word string) (*models.C11yWordsResponseIndividualWordsItems0, error) {
	info, err := i.individualWordInfo(ctx, word)
	if err != nil {
		return nil, err
	}

	return &models.C11yWordsResponseIndividualWordsItems0{
		Word:   word,
		InC11y: true,
		Info:   info,
	}, nil
}

func (i *Inspector) individualWordInfo(ctx context.Context,
	word string) (*models.C11yWordsResponseIndividualWordsItems0Info, error) {
	vector, err := i.client.VectorForWord(ctx, word)
	if err != nil {
		return nil, err
	}

	nns, err := i.nearestNeighbors(ctx, vector)
	if err != nil {
		return nil, err
	}

	return &models.C11yWordsResponseIndividualWordsItems0Info{
		Vector:           vector,
		NearestNeighbors: nns,
	}, nil
}

// Splits a CamelCase string to an array
// Based on: https://github.com/fatih/camelcase
func split(src string) (entries []string) {
	// don't split invalid utf8
	if !utf8.ValidString(src) {
		return []string{src}
	}
	entries = []string{}
	var runes [][]rune
	lastClass := 0
	class := 0
	// split into fields based on class of unicode character
	for _, r := range src {
		switch true {
		case unicode.IsLower(r):
			class = 1
		case unicode.IsUpper(r):
			class = 2
		case unicode.IsDigit(r):
			class = 3
		default:
			class = 4
		}
		if class == lastClass {
			runes[len(runes)-1] = append(runes[len(runes)-1], r)
		} else {
			runes = append(runes, []rune{r})
		}
		lastClass = class
	}
	// handle upper case -> lower case sequences, e.g.
	// "PDFL", "oader" -> "PDF", "Loader"
	for i := 0; i < len(runes)-1; i++ {
		if unicode.IsUpper(runes[i][0]) && unicode.IsLower(runes[i+1][0]) {
			runes[i+1] = append([]rune{runes[i][len(runes[i])-1]}, runes[i+1]...)
			runes[i] = runes[i][:len(runes[i])-1]
		}
	}
	// construct []string from results
	for _, s := range runes {
		if len(s) > 0 {
			entries = append(entries, strings.ToLower(string(s)))
		}
	}
	return
}
