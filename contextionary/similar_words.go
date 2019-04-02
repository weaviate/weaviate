package contextionary

import (
	"regexp"
)

const simliarWordsLimit = 15

func safeGetSimilarWordsFromAny(c11y Contextionary, word string, n, k int) ([]string, []float32) {
	i := c11y.WordToItemIndex(word)
	if !i.IsPresent() {
		return []string{word}, []float32{1}
	}

	indices, newCertainties, err := c11y.GetNnsByItem(i, n, k)
	if err != nil {
		return []string{word}, []float32{1}
	}

	var words []string
	var certainties []float32
	count := 0
	for i, index := range indices {
		if count >= simliarWordsLimit {
			break
		}

		count++
		word, err := c11y.ItemIndexToWord(index)
		if err != nil {
			continue
		}

		if wordHasIllegalCharacters(word) {
			continue
		}

		words = append(words, word)
		certainties = append(certainties, newCertainties[i])
	}

	return words, certainties
}

func safeGetSimilarWordsWithCertaintyFromAny(c11y Contextionary, word string, certainty float32) []string {
	var matchingWords []string
	var matchtingCertainties []float32

	words, certainties := c11y.SafeGetSimilarWords(word, 100, 32)
	for i, word := range words {
		var dist float32
		if dist = DistanceToCertainty(certainties[i]); dist < certainty {
			continue
		}

		matchingWords = append(matchingWords, alphanumeric(word))
		matchtingCertainties = append(matchtingCertainties, dist)
	}

	return matchingWords
}

func wordHasIllegalCharacters(word string) bool {
	// we know that the schema based contextionary uses a leading dollar sign for
	// the class and property centroids, so we can easily filter them out
	return regexp.MustCompile("^\\$").MatchString(word)
}

func alphanumeric(word string) string {
	return regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(word, "")
}
