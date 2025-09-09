//go:build tools

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

package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
)

func main() {
	var (
		filePath = flag.String("file", "", "Path to the commit log file to repair")
		verbose  = flag.Bool("verbose", false, "Enable verbose logging")
	)
	flag.Parse()

	if *filePath == "" {
		log.Fatal("Please provide a file path using -file flag")
	}

	// Check if file exists
	if _, err := os.Stat(*filePath); os.IsNotExist(err) {
		log.Fatalf("File does not exist: %s", *filePath)
	}

	// Setup logging
	logger := logrus.New()
	if *verbose {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	// Open the file
	fd, err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", *filePath, err)
	}
	defer fd.Close()

	// Get file info
	info, err := fd.Stat()
	if err != nil {
		log.Fatalf("Failed to get file info for %s: %v", *filePath, err)
	}

	if info.Size() == 0 {
		log.Printf("File %s is empty, nothing to do", *filePath)
		return
	}

	log.Printf("Processing file: %s (size: %d bytes)", *filePath, info.Size())

	// Create a buffered reader
	fdBuf := bufio.NewReaderSize(fd, 256*1024)

	// Create deserializer and process the file
	deserializer := hnsw.NewDeserializer(logger)
	state, valid, err := deserializer.Do(fdBuf, nil, false)

	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF || strings.Contains(err.Error(), "unrecognized commit type") {
			log.Printf("File ended abruptly, truncating to valid length: %d bytes", valid)

			// Close the file before truncating
			fd.Close()

			// Truncate the file to its valid length
			if err := os.Truncate(*filePath, int64(valid)); err != nil {
				log.Fatalf("Failed to truncate file %s: %v", *filePath, err)
			}

			log.Printf("Successfully truncated file %s to %d bytes", *filePath, valid)
		} else {
			log.Fatalf("Deserialization error: %v", err)
		}
	} else {
		log.Printf("File processed successfully, valid length: %d bytes", valid)
	}

	// Print some statistics about what was deserialized
	if state != nil {
		log.Printf("Deserialization results:")
		log.Printf("  - Nodes: %d", len(state.Nodes))
		log.Printf("  - Deleted nodes: %d", len(state.NodesDeleted))
		log.Printf("  - Entrypoint: %d", state.Entrypoint)
		log.Printf("  - Level: %d", state.Level)
		log.Printf("  - Tombstones: %d", len(state.Tombstones))
		log.Printf("  - Deleted tombstones: %d", len(state.TombstonesDeleted))
		log.Printf("  - Links replaced: %d", len(state.LinksReplaced))
		log.Printf("  - Entrypoint changed: %v", state.EntrypointChanged)
		log.Printf("  - Compressed: %v", state.Compressed)
	}
}
