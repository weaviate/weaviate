/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package contextionary

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	annoy "github.com/creativesoftwarefdn/weaviate/contextionary/annoyindex"
)

type mmappedIndex struct {
	word_index *Wordlist
	knn        annoy.AnnoyIndex
	wordOcc    []wordOccurence
}

type wordOccurence struct {
	idx   ItemIndex
	place int32
}

func (m *mmappedIndex) GetNumberOfItems() int {
	return int(m.word_index.numberOfWords)
}

// Returns the length of the used vectors.
func (m *mmappedIndex) GetVectorLength() int {
	return int(m.word_index.vectorWidth)
}

func (m *mmappedIndex) WordToItemIndex(word string) ItemIndex {
	return m.word_index.FindIndexByWord(word)
}

func (m *mmappedIndex) ItemIndexToWord(item ItemIndex) (string, error) {
	if item >= 0 && item <= m.word_index.GetNumberOfWords() {
		return m.word_index.getWord(item), nil
	} else {
		return "", fmt.Errorf("Index out of bounds")
	}
}

func (m *mmappedIndex) GetVectorForItemIndex(item ItemIndex) (*Vector, error) {

	if item >= 0 && item <= m.word_index.GetNumberOfWords() {
		var floats []float32
		m.knn.GetItem(int(item), &floats)

		return &Vector{floats}, nil
	} else {
		return nil, fmt.Errorf("Index out of bounds")
	}
}

// Compute the distance between two items.
func (m *mmappedIndex) GetDistance(a ItemIndex, b ItemIndex) (float32, error) {
	if a >= 0 && b >= 0 && a <= m.word_index.GetNumberOfWords() && b <= m.word_index.GetNumberOfWords() {
		return m.knn.GetDistance(int(a), int(b)), nil
	} else {
		return 0, fmt.Errorf("Index out of bounds")
	}
}

func (m *mmappedIndex) GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32, error) {
	if item >= 0 && item <= m.word_index.GetNumberOfWords() {
		var items []int
		var distances []float32

		m.knn.GetNnsByItem(int(item), n, k, &items, &distances)

		var indices []ItemIndex = make([]ItemIndex, len(items))
		for i, x := range items {
			indices[i] = ItemIndex(x)
		}

		return indices, distances, nil
	} else {
		return nil, nil, fmt.Errorf("Index out of bounds")
	}
}

func (m *mmappedIndex) GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32, error) {
	if len(vector.vector) == m.GetVectorLength() {
		var items []int
		var distances []float32

		m.knn.GetNnsByVector(vector.vector, n, k, &items, &distances)

		var indices []ItemIndex = make([]ItemIndex, len(items))
		for i, x := range items {
			indices[i] = ItemIndex(x)
		}

		return indices, distances, nil
	} else {
		return nil, nil, fmt.Errorf("Wrong vector length provided")
	}
}

func (m *mmappedIndex) SentenceToItemIndex(sentence string) (Vector, error) {

	// create an empty array which will contain all the word locations in the contextionary
	wordVectors := []Vector{}

	// refactor words to lowercase based on: ^[A-Za-z][\ A-Za-z0-9]*$ in generator
	for _, word := range strings.Split(sentence, " ") {

		// compile allowed words
		reg, err := regexp.Compile("[^A-Za-z0-9]+")
		if err != nil {
			return Vector{}, err
		}

		// replace none a-z characters => all to lowercase => get word index
		wordIndex := m.WordToItemIndex(strings.ToLower(reg.ReplaceAllString(word, "")))

		// append if item is available
		if wordIndex != -1 {
			vector, err := m.GetVectorForItemIndex(wordIndex)
			if err != nil {
				return Vector{}, err
			}
			wordVectors = append(wordVectors, *vector)
		}

	}

	// compute the centroid of the sentence
	centroidOfSentence, err := ComputeCentroid(wordVectors)
	if err != nil {
		return Vector{}, err
	}

	return *centroidOfSentence, nil
}

func vectorFileAvailable(file string) bool {
	if _, err := os.Stat(file); err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	} else {
		return false
	}

}

func loadVectorFromDisk(annoy_index string, word_index_file_name string, word_vocab_file_name string) (Contextionary, error) {

	// validate if files exsist
	if vectorFileAvailable(annoy_index) == false {
		return nil, fmt.Errorf("Could not load file: %+v", annoy_index)
	} else if vectorFileAvailable(word_index_file_name) == false {
		return nil, fmt.Errorf("Could not load file: %+v", word_index_file_name)
	} else if vectorFileAvailable(word_vocab_file_name) == false {
		return nil, fmt.Errorf("Could not load file: %+v", word_vocab_file_name)
	}

	word_index, err := LoadWordlist(word_index_file_name)

	if err != nil {
		return nil, fmt.Errorf("Could not load vector: %+v", err)
	}

	knn := annoy.NewAnnoyIndexEuclidean(int(word_index.vectorWidth))
	knn.Load(annoy_index)

	// TEMP
	var occurenceList []wordOccurence

	// create empty returner array
	returnWordOccurence := []wordOccurence{}

	file, err := os.Open(word_vocab_file_name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		// split per word to string and int32
		wordSplit := strings.Split(scanner.Text(), " ")

		if len(wordSplit) == 2 {

			singleWordOccInt64, err := strconv.ParseInt(wordSplit[1], 10, 32)
			if err != nil {
				return nil, err
			}
			fmt.Println(wordSplit[0])
			// convert to int32
			singleWordIdx := word_index.FindIndexByWord(wordSplit[0])
			singleWordOcc := int32(singleWordOccInt64)

			if singleWordIdx != -1 {

				returnWordOccurence = append(returnWordOccurence, wordOccurence{
					idx:   singleWordIdx,
					place: singleWordOcc,
				})
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// END TEMP

	idx := &mmappedIndex{
		word_index: word_index,
		knn:        knn,
		wordOcc:    occurenceList,
	}

	return idx, nil
}

func getFilenameFromURL(URL string) string {
	fileRequest, _ := http.NewRequest("GET", URL, nil)
	return path.Base(fileRequest.URL.Path)
}

func downloadContextionaryFile(URL string, tmpFolder string) error {

	// filename to save
	fileName := tmpFolder + getFilenameFromURL(URL)

	// remove filename if exists
	_ = os.Remove(fileName)

	// create tmp dir if not exists
	if _, err := os.Stat(tmpFolder); os.IsNotExist(err) {
		os.Mkdir(tmpFolder, 0775)
	}

	// Get the data
	log.Print("Downloading: " + URL)
	resp, err := http.Get(URL)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("Can't download: " + URL)
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)

	return nil
}

func isValidUrl(toTest string) bool {
	URL, err := url.Parse(toTest)
	if err != nil {
		return false
	} else if URL.Scheme != "http" && URL.Scheme != "https" && URL.Scheme != "ftp" {
		return false
	}
	return true
}

// Downloads file if valid URL
func downloadOrNot(i string, tmpFolder string) (string, error) {
	// check if annoy_index = url
	if isValidUrl(i) == true {
		err := downloadContextionaryFile(i, tmpFolder)
		if err != nil {
			return "", err
		}
		return tmpFolder + getFilenameFromURL(i), nil
	}
	return i, nil
}

func LoadVector(annoy_index string, word_index_file_name string, word_vocab_file_name string) (Contextionary, error) {

	// set tmp folder
	tmpFolder := "./tmp/"

	// validate annoy_index_file
	annoy_index_file, err := downloadOrNot(annoy_index, tmpFolder)
	if err != nil {
		return nil, err
	}

	// validate word_index_file_name
	word_index_file_name_file, err := downloadOrNot(word_index_file_name, tmpFolder)
	if err != nil {
		return nil, err
	}

	// validate word_vocab_file_name
	word_vocab_file_name, err = downloadOrNot(word_vocab_file_name, tmpFolder)
	if err != nil {
		return nil, err
	}

	return loadVectorFromDisk(annoy_index_file, word_index_file_name_file, word_vocab_file_name)
}
