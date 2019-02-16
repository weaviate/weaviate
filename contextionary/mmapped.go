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
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"

	annoy "github.com/creativesoftwarefdn/weaviate/contextionary/annoyindex"
)

type mmappedIndex struct {
	word_index *Wordlist
	knn        annoy.AnnoyIndex
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

func loadVectorFromDisk(annoy_index string, word_index_file_name string) (Contextionary, error) {
	word_index, err := LoadWordlist(word_index_file_name)

	if err != nil {
		return nil, fmt.Errorf("Could not load vector: %+v", err)
	}

	knn := annoy.NewAnnoyIndexEuclidean(int(word_index.vectorWidth))
	knn.Load(annoy_index)

	idx := &mmappedIndex{
		word_index: word_index,
		knn:        knn,
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
	_, err := url.ParseRequestURI(toTest)
	if err != nil {
		return false
	} else {
		return true
	}
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

func LoadVector(annoy_index string, word_index_file_name string) (Contextionary, error) {

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

	return loadVectorFromDisk(annoy_index_file, word_index_file_name_file)
}
