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
package generator

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	annoy "github.com/creativesoftwarefdn/weaviate/contextionary/annoyindex"
	"github.com/syndtr/goleveldb/leveldb"
)

type Options struct {
	VectorCSVPath string `short:"c" long:"vector-csv-path" description:"Path to the output file of Glove" required:"true"`
	TempDBPath    string `short:"t" long:"temp-db-path" description:"Location for the temporary database" default:".tmp_import"`
	OutputPrefix  string `short:"p" long:"output-prefix" description:"The prefix of the names of the files" required:"true"`
	K             int    `short:"k" description:"number of forrests to generate" default:"20"`
}

type WordVectorInfo struct {
	numberOfWords int
	vectorWidth   int
	k             int
	metadata      JsonMetadata
}

type JsonMetadata struct {
	K int `json:"k"` // the number of parallel forrests.
}

func Generate(options Options) {
	db, err := leveldb.OpenFile(options.TempDBPath, nil)
	defer db.Close()

	if err != nil {
		log.Fatalf("Could not open temporary database file %+v", err)
	}

	file, err := os.Open(options.VectorCSVPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	log.Print("Processing and ordering raw trained data")
	info := readVectorsFromFileAndInsertIntoLevelDB(db, file, options.OutputPrefix+".vocab")

	info.k = options.K
	info.metadata = JsonMetadata{options.K}

	log.Print("Generating wordlist")
	createWordList(db, info, options.OutputPrefix+".idx")

	log.Print("Generating k-nn index")
	createKnn(db, info, options.OutputPrefix+".knn")

	db.Close()
	os.RemoveAll(options.TempDBPath)
}

// Validates if all words are availabe, returns true if available
func Validate(fileName string) bool {
	c13y, err := contextionary.LoadVectorFromDisk(fileName+".knn", fileName+".idx")
	if err != nil {
		log.Fatal(err)
	}

	// load the vocab file line by line
	vocabFile, err := os.Open(fileName + ".vocab")
	if err != nil {
		log.Fatal(err)
	}
	defer vocabFile.Close()
	scanner := bufio.NewScanner(vocabFile)

	// loop over the words
	for scanner.Scan() {
		wordToCheck := scanner.Text()

		// check if the word is present
		itemIndex := c13y.WordToItemIndex(wordToCheck)
		if ok := itemIndex.IsPresent(); !ok {
			log.Fatal(fmt.Errorf("item index for %s is not present", wordToCheck))
			return false
		}
	}

	return true
}

// read word vectors, insert them into level db, also return the dimension of the vectors.
func readVectorsFromFileAndInsertIntoLevelDB(db *leveldb.DB, file *os.File, words_file string) WordVectorInfo {
	var vector_length int = -1
	var nr_words int = 0
	var add_words int64 = 0
	var skipped_words int64 = 0

	scanner := bufio.NewScanner(file)

	// open the words file
	f, err := os.OpenFile(words_file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}

	for scanner.Scan() {
		nr_words += 1
		parts := strings.Split(scanner.Text(), " ")

		word := parts[0]
		if vector_length == -1 {
			vector_length = len(parts) - 1
		}

		if vector_length != len(parts)-1 {
			log.Print("Line corruption found for the word [" + word + "]. Lenght expected " + strconv.Itoa(vector_length) + " but found " + strconv.Itoa(len(parts)) + ". Word will be skipped.")
			continue
		}

		// validate if the word should be used, this includes capitals for names but no other than that
		var validRegex = regexp.MustCompile(`^[A-Za-z][\ A-Za-z0-9]*$`)
		if validRegex.MatchString(word) == false {
			skipped_words++
			continue
		} else {
			// add word to the available word file
			if _, err = f.WriteString(word + "\n"); err != nil {
				log.Fatal("can't write to word file")
			}
			add_words++
		}

		// pre-allocate a vector for speed.
		vector := make([]float32, vector_length)

		for i := 1; i <= vector_length; i++ {
			float, err := strconv.ParseFloat(parts[i], 64)

			if err != nil {
				log.Fatal("Error parsing float")
			}

			vector[i-1] = float32(float)
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(vector); err != nil {
			log.Fatal("Could not encode vector for temp db storage")
		}

		db.Put([]byte(word), buf.Bytes(), nil)
	}

	log.Print("Added: ", strconv.FormatInt(add_words, 10), " skipped: ", strconv.FormatInt(skipped_words, 10))

	// close words file
	f.Close()

	return WordVectorInfo{numberOfWords: nr_words, vectorWidth: vector_length}
}

func createWordList(db *leveldb.DB, info WordVectorInfo, outputFileName string) {
	file, err := os.Create(outputFileName)
	if err != nil {
		log.Fatal("Could not open wordlist output file")
	}
	defer file.Close()

	wbuf := bufio.NewWriter(file)

	// Write file header
	err = binary.Write(wbuf, binary.LittleEndian, uint64(info.numberOfWords))
	if err != nil {
		log.Fatal("Could not write length of wordlist.")
	}

	err = binary.Write(wbuf, binary.LittleEndian, uint64(info.vectorWidth))
	if err != nil {
		log.Fatal("Could not write with of the vector.")
	}

	metadata, err := json.Marshal(info.metadata)
	if err != nil {
		log.Fatal("Could not serialize metadata.")
	}

	err = binary.Write(wbuf, binary.LittleEndian, uint64(len(metadata)))
	if err != nil {
		log.Fatal("Could not write with of the vector.")
	}

	_, err = wbuf.Write(metadata)
	if err != nil {
		log.Fatal("Could not write the metadata")
	}

	var metadata_len = uint64(len(metadata))
	var metadata_padding = 4 - (metadata_len % 4)
	for i := 0; uint64(i) < metadata_padding; i++ {
		wbuf.WriteByte(byte(0))
	}

	var word_offset uint64 = (2 + uint64(info.numberOfWords)) * 8 // first two uint64's from the header, then the table of indices.
	word_offset += 8 + metadata_len + metadata_padding            // and the metadata length + content & padding

	var orig_word_offset = word_offset

	// Iterate first time over all data, computing indices for all words.
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		word := string(key)
		length := len(word)
		err = binary.Write(wbuf, binary.LittleEndian, uint64(word_offset))

		if err != nil {
			log.Fatal("Could not write word offset to wordlist")
		}

		word_offset += uint64(length) + 1

		// ensure padding on 4-bytes aligned memory
		padding := 4 - (word_offset % 4)
		word_offset += padding
	}

	iter.Release()
	word_offset = orig_word_offset

	// Iterate second time over all data, now inserting the words
	iter = db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		word := string(key)
		length := len(word)
		wbuf.Write([]byte(word))
		wbuf.WriteByte(byte(0))
		word_offset += uint64(length) + 1

		// ensure padding on 4-bytes aligned memory
		padding := 4 - (word_offset % 4)
		for i := 0; uint64(i) < padding; i++ {
			wbuf.WriteByte(byte(0))
		}

		word_offset += padding
	}
	wbuf.Flush()
	iter.Release()
}

func createKnn(db *leveldb.DB, info WordVectorInfo, outputFileName string) {
	var knn annoy.AnnoyIndex = annoy.NewAnnoyIndexEuclidean(info.vectorWidth)
	var idx int = -1

	iter := db.NewIterator(nil, nil)

	for iter.Next() {
		idx += 1

		vector := make([]float32, info.vectorWidth)
		err := gob.NewDecoder(bytes.NewBuffer(iter.Value())).Decode(&vector)
		if err != nil {
			log.Fatalf("Could not decode vector value %+v", err)
		}
		knn.AddItem(idx, vector)
	}

	knn.Build(info.k) // Hardcoded for now. Must be tweaked.
	knn.Save(outputFileName)
	knn.Unload()
}
