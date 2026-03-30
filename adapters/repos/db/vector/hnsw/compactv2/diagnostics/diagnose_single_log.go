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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compactv2"
)

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	// Read the last .condensed file where the tombstone appears
	logPath := "test_data/1754018183.condensed"

	fmt.Printf("=== Reading %s ===\n", logPath)
	logFile, _ := os.Open(logPath)
	logReader := bufio.NewReaderSize(logFile, 256*1024)
	walReader := compactv2.NewWALCommitReader(logReader, logger)
	memReader := compactv2.NewInMemoryReader(walReader, logger)
	condensedResult, _ := memReader.Do(nil, true)
	logFile.Close()

	fmt.Printf("Condensed result: %d tombstones, %d tombstones deleted\n",
		len(condensedResult.Tombstones), len(condensedResult.TombstonesDeleted))

	// Check if node 29273161 is in tombstones
	targetID := uint64(29273161)
	_, hasTombstone := condensedResult.Tombstones[targetID]
	_, tombstoneDeleted := condensedResult.TombstonesDeleted[targetID]

	fmt.Printf("\nNode %d state:\n", targetID)
	fmt.Printf("  In Tombstones: %v\n", hasTombstone)
	fmt.Printf("  In TombstonesDeleted: %v\n", tombstoneDeleted)

	// Now convert to .sorted and read it back using SafeFileWriter for crash safety
	fmt.Printf("\n=== Converting to .sorted ===\n")
	testPath := "test_data/1754018183.sorted.test"
	defer os.Remove(testPath)

	sfw, err := compactv2.NewSafeFileWriter(testPath, compactv2.DefaultBufferSize)
	if err != nil {
		panic(fmt.Errorf("create safe file writer: %w", err))
	}
	sortedWriter := compactv2.NewSortedWriter(sfw.Writer(), logger)
	if err := sortedWriter.WriteAll(condensedResult); err != nil {
		sfw.Abort()
		panic(fmt.Errorf("write sorted: %w", err))
	}
	if err := sfw.Commit(); err != nil {
		panic(fmt.Errorf("commit sorted: %w", err))
	}

	fmt.Printf("\n=== Reading back .sorted ===\n")
	sortedFile2, _ := os.Open(testPath)
	sortedReader := bufio.NewReaderSize(sortedFile2, 256*1024)
	sortedWalReader := compactv2.NewWALCommitReader(sortedReader, logger)
	sortedMemReader := compactv2.NewInMemoryReader(sortedWalReader, logger)
	sortedResult, _ := sortedMemReader.Do(nil, true)
	sortedFile2.Close()

	fmt.Printf("Sorted result: %d tombstones, %d tombstones deleted\n",
		len(sortedResult.Tombstones), len(sortedResult.TombstonesDeleted))

	_, hasTombstone2 := sortedResult.Tombstones[targetID]
	_, tombstoneDeleted2 := sortedResult.TombstonesDeleted[targetID]

	fmt.Printf("\nNode %d state after conversion:\n", targetID)
	fmt.Printf("  In Tombstones: %v\n", hasTombstone2)
	fmt.Printf("  In TombstonesDeleted: %v\n", tombstoneDeleted2)

	// Trace all operations for this node in both files
	fmt.Printf("\n=== Tracing all operations for node %d ===\n", targetID)

	fmt.Println("\n--- In original .condensed ---")
	logFile3, _ := os.Open(logPath)
	logReader3 := bufio.NewReaderSize(logFile3, 256*1024)
	walReader3 := compactv2.NewWALCommitReader(logReader3, logger)
	for {
		commit, err := walReader3.ReadNextCommit()
		if err != nil {
			break
		}

		switch c := commit.(type) {
		case *compactv2.AddNodeCommit:
			if c.ID == targetID {
				fmt.Printf("  AddNode(id=%d, level=%d)\n", c.ID, c.Level)
			}
		case *compactv2.DeleteNodeCommit:
			if c.ID == targetID {
				fmt.Printf("  DeleteNode(id=%d)\n", c.ID)
			}
		case *compactv2.AddTombstoneCommit:
			if c.ID == targetID {
				fmt.Printf("  AddTombstone(id=%d)\n", c.ID)
			}
		case *compactv2.RemoveTombstoneCommit:
			if c.ID == targetID {
				fmt.Printf("  RemoveTombstone(id=%d)\n", c.ID)
			}
		case *compactv2.ReplaceLinksAtLevelCommit:
			if c.Source == targetID {
				fmt.Printf("  ReplaceLinksAtLevel(source=%d, level=%d, targets=%d links)\n", c.Source, c.Level, len(c.Targets))
			}
		}
	}
	logFile3.Close()

	fmt.Println("\n--- In converted .sorted ---")
	sortedFile3, _ := os.Open(testPath)
	sortedReader3 := bufio.NewReaderSize(sortedFile3, 256*1024)
	sortedWalReader3 := compactv2.NewWALCommitReader(sortedReader3, logger)
	for {
		commit, err := sortedWalReader3.ReadNextCommit()
		if err != nil {
			break
		}

		switch c := commit.(type) {
		case *compactv2.AddNodeCommit:
			if c.ID == targetID {
				fmt.Printf("  AddNode(id=%d, level=%d)\n", c.ID, c.Level)
			}
		case *compactv2.DeleteNodeCommit:
			if c.ID == targetID {
				fmt.Printf("  DeleteNode(id=%d)\n", c.ID)
			}
		case *compactv2.AddTombstoneCommit:
			if c.ID == targetID {
				fmt.Printf("  AddTombstone(id=%d)\n", c.ID)
			}
		case *compactv2.RemoveTombstoneCommit:
			if c.ID == targetID {
				fmt.Printf("  RemoveTombstone(id=%d)\n", c.ID)
			}
		case *compactv2.ReplaceLinksAtLevelCommit:
			if c.Source == targetID {
				fmt.Printf("  ReplaceLinksAtLevel(source=%d, level=%d, targets=%d links)\n", c.Source, c.Level, len(c.Targets))
			}
		}
	}
	sortedFile3.Close()
}
