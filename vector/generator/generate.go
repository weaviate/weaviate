package main

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"os"
	annoy "github.com/creativesoftwarefdn/weaviate/vector/annoyindex"
	flags "github.com/jessevdk/go-flags"
  "bytes"
  "strconv"
  "strings"
)

type Options struct {
	VectorCSVPath string `short:"c" long:"vector-csv-path" description:"Path to the output file of Glove" required:"true"`
  TempDBPath    string `short:"t" long:"temp-db-path" description:"Location for the temporary database" default:".tmp_import"`
  OutputPrefix  string `short:"p" long:"output-prefix" description:"The prefix of the names of the files" required:"true"`
}

type WordVectorInfo struct {
  numberOfWords int
  vectorWidth int
}

func main() {
	var options Options
	var parser = flags.NewParser(&options, flags.Default)

	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}

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

	log.Print("Generating wordlist")
	createWordList(db, info, options.OutputPrefix + ".idx")

	log.Print("Generating k-nn index")
	createKnn(db, info, options.OutputPrefix + ".knn")

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
      print("SETTING VECTOR LENGTH TO ", vector_length, "\n")
		}

		if vector_length != len(parts)-1 {
			log.Fatal("Data corruption; not all words have the same vector length")
		}

		// pre-allocate a vector for speed.
		vector := make([]float32, vector_length)

		for i := 1; i < vector_length; i++ {
			float, err := strconv.ParseFloat(parts[i], 32)

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

  return WordVectorInfo { numberOfWords: nr_words, vectorWidth: vector_length }
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

	var word_offset uint64 = (2 + uint64(info.numberOfWords)) * 8 // first two uint64's from the header, then the table of indices.
  var orig_word_offset = word_offset

  // Iterate first time over all data, computing indices for all words.
  iter := db.NewIterator(nil,nil)
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
  iter = db.NewIterator(nil,nil)
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
  iter.Release()
}

func createKnn(db *leveldb.DB, info WordVectorInfo, outputFileName string) {
  var knn annoy.AnnoyIndex = annoy.NewAnnoyIndexManhattan(300)
  var idx int = -1

  iter := db.NewIterator(nil,nil)

  for iter.Next() {
    idx += 1

    vector := make([]float32, info.vectorWidth)
    err := gob.NewDecoder(bytes.NewBuffer(iter.Value())).Decode(&vector)
    if err != nil {
      log.Fatalf("Could not decode vector value %+v", err)
    }
    knn.AddItem(idx, vector)
  }

  knn.Build(20) // Hardcoded for now. Must be tweaked.
  knn.Save(outputFileName)
  knn.Unload()
}
