//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build ignore
// +build ignore

package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compactv2"
)

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	// Find all .condensed files
	condensedFiles, err := filepath.Glob("test_data/*.condensed")
	if err != nil {
		panic(err)
	}
	sort.Strings(condensedFiles)

	fmt.Printf("Regenerating .sorted files for %d .condensed files...\n\n", len(condensedFiles))

	for i, condensedPath := range condensedFiles {
		base := condensedPath[:len(condensedPath)-len(".condensed")]
		sortedPath := base + ".sorted"

		fmt.Printf("[%d/%d] Processing %s...\n", i+1, len(condensedFiles), filepath.Base(condensedPath))

		// Read .condensed file
		logFile, err := os.Open(condensedPath)
		if err != nil {
			panic(err)
		}

		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		memReader := compactv2.NewInMemoryReader(walReader, logger)
		res, err := memReader.Do(nil, true)
		if err != nil {
			panic(err)
		}
		logFile.Close()

		// Write .sorted file using SafeFileWriter for crash safety
		sfw, err := compactv2.NewSafeFileWriter(sortedPath, compactv2.DefaultBufferSize)
		if err != nil {
			panic(fmt.Errorf("create safe file writer for %s: %w", sortedPath, err))
		}

		sortedWriter := compactv2.NewSortedWriter(sfw.Writer(), logger)
		if err := sortedWriter.WriteAll(res); err != nil {
			sfw.Abort()
			panic(fmt.Errorf("write sorted file %s: %w", sortedPath, err))
		}

		if err := sfw.Commit(); err != nil {
			panic(fmt.Errorf("commit sorted file %s: %w", sortedPath, err))
		}

		fmt.Printf("  ✓ Created %s (tombstones: %d, deleted: %d)\n",
			filepath.Base(sortedPath), len(res.Tombstones), len(res.TombstonesDeleted))
	}

	fmt.Println("\n✓ All .sorted files regenerated successfully!")
}
