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
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	// Find all .condensed files
	condensedFiles, err := filepath.Glob("test_data/*.condensed")
	if err != nil {
		panic(err)
	}
	sort.Strings(condensedFiles)

	fmt.Println("=== Step 1: Read all .condensed files sequentially ===")
	var condensedResult *ent.DeserializationResult
	for i, logPath := range condensedFiles {
		fmt.Printf("Reading %d/%d: %s...\n", i+1, len(condensedFiles), filepath.Base(logPath))
		logFile, _ := os.Open(logPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		memReader := compactv2.NewInMemoryReader(walReader, logger)
		condensedResult, _ = memReader.Do(condensedResult, true)
		logFile.Close()
	}

	fmt.Printf("Condensed result: %d tombstones, %d tombstones deleted\n",
		len(condensedResult.Graph.Tombstones), len(condensedResult.Graph.TombstonesDeleted))

	// Find all .sorted files
	sortedFiles, err := filepath.Glob("test_data/*.sorted")
	if err != nil {
		panic(err)
	}
	sort.Strings(sortedFiles)

	fmt.Println("\n=== Step 2: Read all .sorted files sequentially ===")
	var sortedResult *ent.DeserializationResult
	for i, logPath := range sortedFiles {
		fmt.Printf("Reading %d/%d: %s...\n", i+1, len(sortedFiles), filepath.Base(logPath))
		logFile, _ := os.Open(logPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		memReader := compactv2.NewInMemoryReader(walReader, logger)
		sortedResult, _ = memReader.Do(sortedResult, true)
		logFile.Close()
	}

	fmt.Printf("Sorted result: %d tombstones, %d tombstones deleted\n",
		len(sortedResult.Graph.Tombstones), len(sortedResult.Graph.TombstonesDeleted))

	// Find differences
	fmt.Println("\n=== Step 3: Find tombstone differences ===")

	missingInSorted := []uint64{}
	for id := range condensedResult.Graph.Tombstones {
		if _, ok := sortedResult.Graph.Tombstones[id]; !ok {
			missingInSorted = append(missingInSorted, id)
		}
	}

	extraInSorted := []uint64{}
	for id := range sortedResult.Graph.Tombstones {
		if _, ok := condensedResult.Graph.Tombstones[id]; !ok {
			extraInSorted = append(extraInSorted, id)
		}
	}

	fmt.Printf("Missing in .sorted: %d tombstones\n", len(missingInSorted))
	fmt.Printf("Extra in .sorted: %d tombstones\n", len(extraInSorted))

	if len(missingInSorted) == 0 && len(extraInSorted) == 0 {
		fmt.Println("✓ Tombstones match!")
		return
	}

	// Pick first differing ID to trace
	var traceID uint64
	if len(missingInSorted) > 0 {
		sort.Slice(missingInSorted, func(i, j int) bool { return missingInSorted[i] < missingInSorted[j] })
		traceID = missingInSorted[0]
		fmt.Printf("\n=== Step 4: Trace all operations for node %d (missing in .sorted) ===\n", traceID)
	} else {
		sort.Slice(extraInSorted, func(i, j int) bool { return extraInSorted[i] < extraInSorted[j] })
		traceID = extraInSorted[0]
		fmt.Printf("\n=== Step 4: Trace all operations for node %d (extra in .sorted) ===\n", traceID)
	}

	// Trace in .condensed files
	fmt.Println("\n--- Operations in .condensed files ---")
	for _, logPath := range condensedFiles {
		logFile, _ := os.Open(logPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)

		foundOps := false
		for {
			commit, err := walReader.ReadNextCommit()
			if err != nil {
				break
			}

			var relevantOp string
			switch c := commit.(type) {
			case *compactv2.AddNodeCommit:
				if c.ID == traceID {
					relevantOp = fmt.Sprintf("AddNode(id=%d, level=%d)", c.ID, c.Level)
				}
			case *compactv2.DeleteNodeCommit:
				if c.ID == traceID {
					relevantOp = fmt.Sprintf("DeleteNode(id=%d)", c.ID)
				}
			case *compactv2.AddTombstoneCommit:
				if c.ID == traceID {
					relevantOp = fmt.Sprintf("AddTombstone(id=%d)", c.ID)
				}
			case *compactv2.RemoveTombstoneCommit:
				if c.ID == traceID {
					relevantOp = fmt.Sprintf("RemoveTombstone(id=%d)", c.ID)
				}
			case *compactv2.AddLinkAtLevelCommit:
				if c.Source == traceID || c.Target == traceID {
					relevantOp = fmt.Sprintf("AddLinkAtLevel(source=%d, target=%d, level=%d)", c.Source, c.Target, c.Level)
				}
			case *compactv2.AddLinksAtLevelCommit:
				if c.Source == traceID {
					relevantOp = fmt.Sprintf("AddLinksAtLevel(source=%d, level=%d, targets=%d links)", c.Source, c.Level, len(c.Targets))
				}
			case *compactv2.ReplaceLinksAtLevelCommit:
				if c.Source == traceID {
					relevantOp = fmt.Sprintf("ReplaceLinksAtLevel(source=%d, level=%d, targets=%d links)", c.Source, c.Level, len(c.Targets))
				}
			}

			if relevantOp != "" {
				if !foundOps {
					fmt.Printf("\nFile: %s\n", filepath.Base(logPath))
					foundOps = true
				}
				fmt.Printf("  %s\n", relevantOp)
			}
		}
		logFile.Close()
	}

	// Trace in .sorted files
	fmt.Println("\n--- Operations in .sorted files ---")
	for _, logPath := range sortedFiles {
		logFile, _ := os.Open(logPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)

		foundOps := false
		for {
			commit, err := walReader.ReadNextCommit()
			if err != nil {
				break
			}

			var relevantOp string
			switch c := commit.(type) {
			case *compactv2.AddNodeCommit:
				if c.ID == traceID {
					relevantOp = fmt.Sprintf("AddNode(id=%d, level=%d)", c.ID, c.Level)
				}
			case *compactv2.DeleteNodeCommit:
				if c.ID == traceID {
					relevantOp = fmt.Sprintf("DeleteNode(id=%d)", c.ID)
				}
			case *compactv2.AddTombstoneCommit:
				if c.ID == traceID {
					relevantOp = fmt.Sprintf("AddTombstone(id=%d)", c.ID)
				}
			case *compactv2.RemoveTombstoneCommit:
				if c.ID == traceID {
					relevantOp = fmt.Sprintf("RemoveTombstone(id=%d)", c.ID)
				}
			case *compactv2.AddLinkAtLevelCommit:
				if c.Source == traceID || c.Target == traceID {
					relevantOp = fmt.Sprintf("AddLinkAtLevel(source=%d, target=%d, level=%d)", c.Source, c.Target, c.Level)
				}
			case *compactv2.AddLinksAtLevelCommit:
				if c.Source == traceID {
					relevantOp = fmt.Sprintf("AddLinksAtLevel(source=%d, level=%d, targets=%d links)", c.Source, c.Level, len(c.Targets))
				}
			case *compactv2.ReplaceLinksAtLevelCommit:
				if c.Source == traceID {
					relevantOp = fmt.Sprintf("ReplaceLinksAtLevel(source=%d, level=%d, targets=%d links)", c.Source, c.Level, len(c.Targets))
				}
			}

			if relevantOp != "" {
				if !foundOps {
					fmt.Printf("\nFile: %s\n", filepath.Base(logPath))
					foundOps = true
				}
				fmt.Printf("  %s\n", relevantOp)
			}
		}
		logFile.Close()
	}
}
