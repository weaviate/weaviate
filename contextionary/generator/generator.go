/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package generator

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"

	annoy "github.com/semi-technologies/weaviate/contextionary/annoyindex"
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
	info := readVectorsFromFileAndInsertIntoLevelDB(db, file)

	info.k = options.K
	info.metadata = JsonMetadata{options.K}

	log.Print("Generating wordlist")
	createWordList(db, info, options.OutputPrefix+".idx")

	log.Print("Generating k-nn index")
	createKnn(db, info, options.OutputPrefix+".knn")

	db.Close()
	os.RemoveAll(options.TempDBPath)
}

// read word vectors, insert them into level db, also return the dimension of the vectors.
func readVectorsFromFileAndInsertIntoLevelDB(db *leveldb.DB, file *os.File) WordVectorInfo {
	var vector_length int = -1
	var nr_words int = 0

	scanner := bufio.NewScanner(file)

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
