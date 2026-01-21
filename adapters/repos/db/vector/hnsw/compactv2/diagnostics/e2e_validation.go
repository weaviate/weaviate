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
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compactv2"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// e2e_test performs a complete end-to-end validation of the compactor:
// 1. Read all .condensed files sequentially (control flow)
// 2. Convert each .condensed to .sorted
// 3. Read all .sorted files sequentially (test flow 1)
// 4. Merge all .sorted files using N-Way merger (test flow 2)
// 5. Compare all three results to ensure they match

func main() {
	// Start pprof server for profiling
	go func() {
		fmt.Println("Starting pprof server on :6060")
		fmt.Println("  CPU profile: go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30")
		fmt.Println("  Heap profile: go tool pprof http://localhost:6060/debug/pprof/heap")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			fmt.Printf("pprof server error: %v\n", err)
		}
	}()

	startTotal := time.Now()
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	fmt.Println("=== Compactor V2 End-to-End Test ===\n")

	// Find all .condensed files
	condensedFiles, err := filepath.Glob("../test_data/*.condensed")
	if err != nil {
		panic(err)
	}
	sort.Strings(condensedFiles)

	if len(condensedFiles) == 0 {
		fmt.Println("❌ No .condensed files found in ../test_data/")
		os.Exit(1)
	}

	fmt.Printf("Found %d .condensed files\n\n", len(condensedFiles))

	// ========================================================================
	// CONTROL FLOW: Read all .condensed files sequentially
	// ========================================================================
	fmt.Println("Step 1: Reading all .condensed files sequentially (CONTROL)...")
	startStep1 := time.Now()
	var controlResult *ent.DeserializationResult
	for i, logPath := range condensedFiles {
		fmt.Printf("  [%d/%d] %s\n", i+1, len(condensedFiles), filepath.Base(logPath))
		logFile, _ := os.Open(logPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		memReader := compactv2.NewInMemoryReader(walReader, logger)
		controlResult, _ = memReader.Do(controlResult, true)
		logFile.Close()
	}
	step1Duration := time.Since(startStep1)

	fmt.Printf("\n✓ Control result (took %v):\n", step1Duration)
	fmt.Printf("  Nodes: %d\n", len(controlResult.Graph.Nodes))
	fmt.Printf("  Tombstones: %d\n", len(controlResult.Graph.Tombstones))
	fmt.Printf("  TombstonesDeleted: %d\n", len(controlResult.Graph.TombstonesDeleted))
	fmt.Printf("  NodesDeleted: %d\n", len(controlResult.Graph.NodesDeleted))
	fmt.Printf("  Entrypoint: %d (level %d)\n\n", controlResult.Graph.Entrypoint, controlResult.Graph.Level)

	// ========================================================================
	// TEST FLOW 1: Convert each .condensed to .sorted, then read sequentially
	// ========================================================================
	fmt.Println("Step 2: Converting .condensed files to .sorted...")
	startStep2 := time.Now()
	sortedFiles := []string{}
	var totalReadTime, totalWriteTime time.Duration
	for i, condensedPath := range condensedFiles {
		fileStart := time.Now()
		fmt.Printf("  [%d/%d] Converting %s...", i+1, len(condensedFiles), filepath.Base(condensedPath))

		// Read .condensed
		readStart := time.Now()
		logFile, _ := os.Open(condensedPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		memReader := compactv2.NewInMemoryReader(walReader, logger)
		res, _ := memReader.Do(nil, true)
		logFile.Close()
		readDuration := time.Since(readStart)
		totalReadTime += readDuration

		// Write .sorted using SafeFileWriter for crash safety
		writeStart := time.Now()
		base := condensedPath[:len(condensedPath)-len(".condensed")]
		sortedPath := base + ".sorted"
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
		writeDuration := time.Since(writeStart)
		totalWriteTime += writeDuration

		fileDuration := time.Since(fileStart)
		fmt.Printf(" took %v (read: %v, write: %v)\n", fileDuration, readDuration, writeDuration)

		sortedFiles = append(sortedFiles, sortedPath)
	}
	step2Duration := time.Since(startStep2)
	fmt.Printf("\n✓ Step 2 completed in %v (total read: %v, total write: %v)\n",
		step2Duration, totalReadTime, totalWriteTime)

	fmt.Println("\nStep 3: Reading all .sorted files sequentially...")
	startStep3 := time.Now()
	var sortedResult *ent.DeserializationResult
	for i, logPath := range sortedFiles {
		fmt.Printf("  [%d/%d] %s\n", i+1, len(sortedFiles), filepath.Base(logPath))
		logFile, _ := os.Open(logPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		memReader := compactv2.NewInMemoryReader(walReader, logger)
		sortedResult, _ = memReader.Do(sortedResult, true)
		logFile.Close()
	}
	step3Duration := time.Since(startStep3)

	fmt.Printf("\n✓ Sequential .sorted result (took %v):\n", step3Duration)
	fmt.Printf("  Nodes: %d\n", len(sortedResult.Graph.Nodes))
	fmt.Printf("  Tombstones: %d\n", len(sortedResult.Graph.Tombstones))
	fmt.Printf("  TombstonesDeleted: %d\n", len(sortedResult.Graph.TombstonesDeleted))
	fmt.Printf("  NodesDeleted: %d\n", len(sortedResult.Graph.NodesDeleted))
	fmt.Printf("  Entrypoint: %d (level %d)\n\n", sortedResult.Graph.Entrypoint, sortedResult.Graph.Level)

	// ========================================================================
	// TEST FLOW 2: Merge all .sorted files using N-Way merger
	// ========================================================================
	fmt.Println("Step 4: Merging all .sorted files using N-Way merger...")
	startStep4 := time.Now()

	// Create iterators for all sorted files
	// Note: Files are sorted by timestamp (oldest first), and iterator ID represents
	// precedence where higher ID = more recent = higher precedence.
	// So the first (oldest) file should get ID=0, and the last (newest) should get the highest ID.
	fmt.Println("  Creating iterators...")
	iteratorStart := time.Now()
	iterators := make([]compactv2.IteratorLike, len(sortedFiles))
	iteratorFiles := make([]*os.File, len(sortedFiles))
	for i, sortedPath := range sortedFiles {
		logFile, _ := os.Open(sortedPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		iterator, err := compactv2.NewIterator(walReader, i, logger)
		if err != nil {
			panic(err)
		}
		iterators[i] = iterator
		iteratorFiles[i] = logFile
	}
	iteratorDuration := time.Since(iteratorStart)

	// Create merger
	fmt.Println("  Creating merger...")
	mergerStart := time.Now()
	merger, err := compactv2.NewNWayMerger(iterators, logger)
	if err != nil {
		panic(err)
	}
	mergerDuration := time.Since(mergerStart)

	// Write merged result using SafeFileWriter for crash safety
	fmt.Println("  Writing merged output...")
	writeStart := time.Now()
	mergedPath := "../test_data/merged_all.sorted"
	mergedSfw, err := compactv2.NewSafeFileWriter(mergedPath, compactv2.DefaultBufferSize)
	if err != nil {
		panic(fmt.Errorf("create safe file writer for %s: %w", mergedPath, err))
	}
	mergedWriter := compactv2.NewWALWriter(mergedSfw.Writer())

	// First write global commits
	for _, commit := range merger.GlobalCommits() {
		if err := writeCommit(mergedWriter, commit); err != nil {
			mergedSfw.Abort()
			panic(fmt.Errorf("write global commit: %w", err))
		}
	}

	// Then write node-specific commits
	nodesProcessed := 0
	for {
		nodeCommits, err := merger.Next()
		if err != nil {
			mergedSfw.Abort()
			panic(err)
		}
		if nodeCommits == nil {
			break
		}
		nodesProcessed++
		for _, commit := range nodeCommits.Commits {
			if err := writeCommit(mergedWriter, commit); err != nil {
				mergedSfw.Abort()
				panic(fmt.Errorf("write commit for node %d: %w", nodeCommits.NodeID, err))
			}
		}
	}
	if err := mergedSfw.Commit(); err != nil {
		panic(fmt.Errorf("commit merged file %s: %w", mergedPath, err))
	}
	writeDuration := time.Since(writeStart)

	// Close all iterator files
	for _, file := range iteratorFiles {
		file.Close()
	}

	step4Duration := time.Since(startStep4)
	fmt.Printf("\n✓ Step 4 completed in %v\n", step4Duration)
	fmt.Printf("  Iterator creation: %v\n", iteratorDuration)
	fmt.Printf("  Merger creation: %v\n", mergerDuration)
	fmt.Printf("  Writing merged output: %v (%d nodes)\n", writeDuration, nodesProcessed)

	// Read back merged result
	fmt.Println("\nStep 5: Reading merged result...")
	startStep5 := time.Now()
	mergedReadFile, _ := os.Open(mergedPath)
	mergedReader := bufio.NewReaderSize(mergedReadFile, 256*1024)
	mergedWalReader := compactv2.NewWALCommitReader(mergedReader, logger)
	mergedMemReader := compactv2.NewInMemoryReader(mergedWalReader, logger)
	mergedResult, _ := mergedMemReader.Do(nil, true)
	mergedReadFile.Close()
	step5Duration := time.Since(startStep5)

	fmt.Printf("\n✓ N-Way merged result (took %v):\n", step5Duration)
	fmt.Printf("  Nodes: %d\n", len(mergedResult.Graph.Nodes))
	fmt.Printf("  Tombstones: %d\n", len(mergedResult.Graph.Tombstones))
	fmt.Printf("  TombstonesDeleted: %d\n", len(mergedResult.Graph.TombstonesDeleted))
	fmt.Printf("  NodesDeleted: %d\n", len(mergedResult.Graph.NodesDeleted))
	fmt.Printf("  Entrypoint: %d (level %d)\n\n", mergedResult.Graph.Entrypoint, mergedResult.Graph.Level)

	// ========================================================================
	// TEST FLOW 3: Write snapshot from merged sorted files, read it back
	// ========================================================================
	// NOTE: We use the INDIVIDUAL .sorted files as input (many files), not the
	// single merged_all.sorted file from Step 4. This tests the snapshot writer's
	// ability to handle multi-file merging directly to snapshot format.
	fmt.Println("Step 6: Writing snapshot from individual .sorted files (via n-way merge)...")
	startStep6 := time.Now()

	// Create fresh iterators for all individual .sorted files
	// (we can't reuse Step 4's iterators as they've been consumed)
	snapshotIterators := make([]compactv2.IteratorLike, len(sortedFiles))
	snapshotIteratorFiles := make([]*os.File, len(sortedFiles))
	for i, sortedPath := range sortedFiles {
		logFile, _ := os.Open(sortedPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		iterator, err := compactv2.NewIterator(walReader, i, logger)
		if err != nil {
			panic(err)
		}
		snapshotIterators[i] = iterator
		snapshotIteratorFiles[i] = logFile
	}

	// Create merger for snapshot
	snapshotMerger, err := compactv2.NewNWayMerger(snapshotIterators, logger)
	if err != nil {
		panic(err)
	}

	// Write snapshot using SafeFileWriter for crash safety
	snapshotPath := "../test_data/merged.snapshot"
	snapshotSfw, err := compactv2.NewSafeFileWriter(snapshotPath, compactv2.DefaultBufferSize)
	if err != nil {
		panic(fmt.Errorf("create safe file writer for %s: %w", snapshotPath, err))
	}
	snapshotWriter := compactv2.NewSnapshotWriter(snapshotSfw.Writer())

	if err := snapshotWriter.WriteFromMerger(snapshotMerger); err != nil {
		snapshotSfw.Abort()
		panic(fmt.Errorf("write snapshot: %w", err))
	}
	if err := snapshotSfw.Commit(); err != nil {
		panic(fmt.Errorf("commit snapshot file %s: %w", snapshotPath, err))
	}

	// Close iterator files
	for _, file := range snapshotIteratorFiles {
		file.Close()
	}

	step6Duration := time.Since(startStep6)
	fmt.Printf("✓ Snapshot written to %s (took %v)\n\n", snapshotPath, step6Duration)

	fmt.Println("Step 7: Reading snapshot back...")
	startStep7 := time.Now()
	snapshotReader := compactv2.NewSnapshotReader(logger)
	snapshotResult, err := snapshotReader.ReadFromFile(snapshotPath)
	if err != nil {
		panic(fmt.Errorf("read snapshot: %w", err))
	}
	step7Duration := time.Since(startStep7)

	fmt.Printf("\n✓ Snapshot result (took %v):\n", step7Duration)
	fmt.Printf("  Nodes: %d\n", len(snapshotResult.Graph.Nodes))
	fmt.Printf("  Tombstones: %d\n", len(snapshotResult.Graph.Tombstones))
	fmt.Printf("  Entrypoint: %d (level %d)\n\n", snapshotResult.Graph.Entrypoint, snapshotResult.Graph.Level)

	// ========================================================================
	// COMPARISON
	// ========================================================================
	fmt.Println("Step 8: Comparing results...")
	startStep8 := time.Now()
	fmt.Println("─────────────────────────────────────────────────────────────")

	allMatch := true

	// Compare Control vs Sequential Sorted
	fmt.Println("\n📊 Control (.condensed) vs Sequential (.sorted):")
	if !compareResults("  ", controlResult, sortedResult) {
		allMatch = false
	}

	// Compare Control vs N-Way Merged
	fmt.Println("\n📊 Control (.condensed) vs N-Way Merged:")
	if !compareResults("  ", controlResult, mergedResult) {
		allMatch = false
	}

	// Compare Sequential Sorted vs N-Way Merged
	fmt.Println("\n📊 Sequential (.sorted) vs N-Way Merged:")
	if !compareResults("  ", sortedResult, mergedResult) {
		allMatch = false
	}

	// Compare Control vs Snapshot (accounting for snapshot limitations)
	fmt.Println("\n📊 Control (.condensed) vs Snapshot:")
	if !compareResultsForSnapshot("  ", controlResult, snapshotResult) {
		allMatch = false
	}

	step8Duration := time.Since(startStep8)
	totalDuration := time.Since(startTotal)

	fmt.Println("\n─────────────────────────────────────────────────────────────")
	fmt.Println("\n⏱️  Performance Summary:")
	fmt.Printf("  Step 1 (Read .condensed):        %v\n", step1Duration)
	fmt.Printf("  Step 2 (Convert to .sorted):     %v\n", step2Duration)
	fmt.Printf("    - Total read time:             %v\n", totalReadTime)
	fmt.Printf("    - Total write time:            %v\n", totalWriteTime)
	fmt.Printf("  Step 3 (Read .sorted):           %v\n", step3Duration)
	fmt.Printf("  Step 4 (Merge to .sorted):       %v\n", step4Duration)
	fmt.Printf("    - Iterator creation:           %v\n", iteratorDuration)
	fmt.Printf("    - Merger creation:             %v\n", mergerDuration)
	fmt.Printf("    - Writing merged output:       %v\n", writeDuration)
	fmt.Printf("  Step 5 (Read merged .sorted):    %v\n", step5Duration)
	fmt.Printf("  Step 6 (Write snapshot):         %v\n", step6Duration)
	fmt.Printf("  Step 7 (Read snapshot):          %v\n", step7Duration)
	fmt.Printf("  Step 8 (Comparisons):            %v\n", step8Duration)
	fmt.Printf("  ─────────────────────────────────────\n")
	fmt.Printf("  Total time:                      %v\n", totalDuration)

	fmt.Println("\n─────────────────────────────────────────────────────────────")
	if allMatch {
		fmt.Println("\n✅ SUCCESS: All results match perfectly!")
		fmt.Println("\n✓ The compactor correctly:")
		fmt.Println("  1. Converts .condensed files to .sorted format")
		fmt.Println("  2. Preserves all tombstone operations")
		fmt.Println("  3. Preserves all node connections/links")
		fmt.Println("  4. Merges multiple .sorted files correctly")
		fmt.Println("  5. Produces identical results via sequential read and merge")
		fmt.Println("  6. Writes and reads snapshots with correct final state")
	} else {
		fmt.Println("\n❌ FAILURE: Results do not match!")
		os.Exit(1)
	}
}

func writeCommit(w *compactv2.WALWriter, c compactv2.Commit) error {
	switch commit := c.(type) {
	case *compactv2.AddNodeCommit:
		return w.WriteAddNode(commit.ID, commit.Level)
	case *compactv2.SetEntryPointMaxLevelCommit:
		return w.WriteSetEntryPointMaxLevel(commit.Entrypoint, commit.Level)
	case *compactv2.AddLinkAtLevelCommit:
		return w.WriteAddLinkAtLevel(commit.Source, commit.Level, commit.Target)
	case *compactv2.AddLinksAtLevelCommit:
		return w.WriteAddLinksAtLevel(commit.Source, commit.Level, commit.Targets)
	case *compactv2.ReplaceLinksAtLevelCommit:
		return w.WriteReplaceLinksAtLevel(commit.Source, commit.Level, commit.Targets)
	case *compactv2.ClearLinksCommit:
		return w.WriteClearLinks(commit.ID)
	case *compactv2.ClearLinksAtLevelCommit:
		return w.WriteClearLinksAtLevel(commit.ID, commit.Level)
	case *compactv2.AddTombstoneCommit:
		return w.WriteAddTombstone(commit.ID)
	case *compactv2.RemoveTombstoneCommit:
		return w.WriteRemoveTombstone(commit.ID)
	case *compactv2.DeleteNodeCommit:
		return w.WriteDeleteNode(commit.ID)
	case *compactv2.ResetIndexCommit:
		return w.WriteResetIndex()
	case *compactv2.AddPQCommit:
		return w.WriteAddPQ(commit.Data)
	case *compactv2.AddSQCommit:
		return w.WriteAddSQ(commit.Data)
	case *compactv2.AddRQCommit:
		return w.WriteAddRQ(commit.Data)
	case *compactv2.AddBRQCommit:
		return w.WriteAddBRQ(commit.Data)
	case *compactv2.AddMuveraCommit:
		return w.WriteAddMuvera(commit.Data)
	default:
		return fmt.Errorf("unrecognized commit type %T", c)
	}
}

func compareResults(indent string, a, b *ent.DeserializationResult) bool {
	match := true

	// Compare tombstones
	if !compareTombstones(indent, a, b) {
		match = false
	}

	// Compare other metadata
	if !compareMetadata(indent, a, b) {
		match = false
	}

	// Compare nodes and their connections
	if !compareNodes(indent, a, b) {
		match = false
	}

	return match
}

func compareTombstones(indent string, a, b *ent.DeserializationResult) bool {
	match := true

	if len(a.Graph.Tombstones) != len(b.Graph.Tombstones) {
		fmt.Printf("%sTombstones: %d vs %d ❌\n", indent, len(a.Graph.Tombstones), len(b.Graph.Tombstones))
		match = false

		// Find differences
		missing := []uint64{}
		for id := range a.Graph.Tombstones {
			if _, ok := b.Graph.Tombstones[id]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			fmt.Printf("%s  Missing in second: %d tombstones (first 5: %v)\n", indent, len(missing), missing[:min(5, len(missing))])
		}

		extra := []uint64{}
		for id := range b.Graph.Tombstones {
			if _, ok := a.Graph.Tombstones[id]; !ok {
				extra = append(extra, id)
			}
		}
		if len(extra) > 0 {
			fmt.Printf("%s  Extra in second: %d tombstones (first 5: %v)\n", indent, len(extra), extra[:min(5, len(extra))])
		}
	} else {
		fmt.Printf("%sTombstones: %d ✓\n", indent, len(a.Graph.Tombstones))
	}

	if len(a.Graph.TombstonesDeleted) != len(b.Graph.TombstonesDeleted) {
		fmt.Printf("%sTombstonesDeleted: %d vs %d ❌\n", indent, len(a.Graph.TombstonesDeleted), len(b.Graph.TombstonesDeleted))
		match = false
	} else {
		fmt.Printf("%sTombstonesDeleted: %d ✓\n", indent, len(a.Graph.TombstonesDeleted))
	}

	return match
}

func compareMetadata(indent string, a, b *ent.DeserializationResult) bool {
	match := true

	if len(a.Graph.NodesDeleted) != len(b.Graph.NodesDeleted) {
		fmt.Printf("%sNodesDeleted: %d vs %d ❌\n", indent, len(a.Graph.NodesDeleted), len(b.Graph.NodesDeleted))
		match = false
	} else {
		fmt.Printf("%sNodesDeleted: %d ✓\n", indent, len(a.Graph.NodesDeleted))
	}

	if a.Graph.Entrypoint != b.Graph.Entrypoint {
		fmt.Printf("%sEntrypoint: %d vs %d ❌\n", indent, a.Graph.Entrypoint, b.Graph.Entrypoint)
		match = false
	} else {
		fmt.Printf("%sEntrypoint: %d ✓\n", indent, a.Graph.Entrypoint)
	}

	if a.Graph.Level != b.Graph.Level {
		fmt.Printf("%sLevel: %d vs %d ❌\n", indent, a.Graph.Level, b.Graph.Level)
		match = false
	} else {
		fmt.Printf("%sLevel: %d ✓\n", indent, a.Graph.Level)
	}

	return match
}

func compareNodes(indent string, a, b *ent.DeserializationResult) bool {
	match := true

	// Count non-nil nodes
	nonNilA := 0
	for _, node := range a.Graph.Nodes {
		if node != nil {
			nonNilA++
		}
	}

	nonNilB := 0
	for _, node := range b.Graph.Nodes {
		if node != nil {
			nonNilB++
		}
	}

	if nonNilA != nonNilB {
		fmt.Printf("%sNon-nil nodes: %d vs %d ❌\n", indent, nonNilA, nonNilB)
		match = false
	} else {
		fmt.Printf("%sNon-nil nodes: %d ✓\n", indent, nonNilA)
	}

	// Compare individual nodes and their connections
	maxLen := len(a.Graph.Nodes)
	if len(b.Graph.Nodes) > maxLen {
		maxLen = len(b.Graph.Nodes)
	}

	mismatchedNodes := 0
	mismatchedLinks := 0
	firstMismatch := true

	for i := uint64(0); i < uint64(maxLen); i++ {
		var nodeA, nodeB *ent.Vertex
		if int(i) < len(a.Graph.Nodes) {
			nodeA = a.Graph.Nodes[i]
		}
		if int(i) < len(b.Graph.Nodes) {
			nodeB = b.Graph.Nodes[i]
		}

		// Both nil - ok
		if nodeA == nil && nodeB == nil {
			continue
		}

		// One nil, one not - mismatch
		if (nodeA == nil) != (nodeB == nil) {
			mismatchedNodes++
			continue
		}

		// Both non-nil - compare properties
		if nodeA.ID != nodeB.ID || nodeA.Level != nodeB.Level {
			mismatchedNodes++
			continue
		}

		// Compare connections
		if !compareConnections(nodeA, nodeB) {
			if firstMismatch {
				// Log detailed info about first mismatch
				fmt.Printf("\n%s🔍 First link mismatch at node %d:\n", indent, i)
				fmt.Printf("%s  ID: %d, Level: %d\n", indent, nodeA.ID, nodeA.Level)

				if nodeA.Connections != nil {
					fmt.Printf("%s  Expected (from .sorted sequential):\n", indent)
					iterA := nodeA.Connections.Iterator()
					for iterA.Next() {
						level, links := iterA.Current()
						fmt.Printf("%s    Level %d: %d links: %v\n", indent, level, len(links), links)
					}
				} else {
					fmt.Printf("%s  Expected connections: nil\n", indent)
				}

				if nodeB.Connections != nil {
					fmt.Printf("%s  Actual (from merged):\n", indent)
					iterB := nodeB.Connections.Iterator()
					for iterB.Next() {
						level, links := iterB.Current()
						fmt.Printf("%s    Level %d: %d links: %v\n", indent, level, len(links), links)
					}
				} else {
					fmt.Printf("%s  Actual connections: nil\n", indent)
				}
				fmt.Println()

				firstMismatch = false
			}
			mismatchedLinks++
		}
	}

	if mismatchedNodes > 0 {
		fmt.Printf("%sNode mismatches: %d ❌\n", indent, mismatchedNodes)
		match = false
	}

	if mismatchedLinks > 0 {
		fmt.Printf("%sLink mismatches: %d nodes ❌\n", indent, mismatchedLinks)
		match = false
	}

	if mismatchedNodes == 0 && mismatchedLinks == 0 {
		fmt.Printf("%sAll node connections match ✓\n", indent)
	}

	return match
}

func compareConnections(a, b *ent.Vertex) bool {
	if a.Connections == nil && b.Connections == nil {
		return true
	}

	if (a.Connections == nil) != (b.Connections == nil) {
		return false
	}

	// Compare number of layers
	if a.Connections.Layers() != b.Connections.Layers() {
		return false
	}

	// Compare links at each level
	iterA := a.Connections.Iterator()
	iterB := b.Connections.Iterator()

	for iterA.Next() && iterB.Next() {
		levelA, linksA := iterA.Current()
		levelB, linksB := iterB.Current()

		if levelA != levelB {
			return false
		}

		if len(linksA) != len(linksB) {
			return false
		}

		// Compare link targets (should be in same order)
		for i := range linksA {
			if linksA[i] != linksB[i] {
				return false
			}
		}
	}

	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// compareResultsForSnapshot compares control results against snapshot results,
// accounting for snapshot format limitations:
//   - Snapshots don't store TombstonesDeleted or NodesDeleted maps
//   - Instead, deleted nodes become nil and deleted tombstones are absent
func compareResultsForSnapshot(indent string, control, snapshot *ent.DeserializationResult) bool {
	match := true

	// Verify entrypoint and level
	if control.Graph.Entrypoint != snapshot.Graph.Entrypoint {
		fmt.Printf("%sEntrypoint: %d vs %d ❌\n", indent, control.Graph.Entrypoint, snapshot.Graph.Entrypoint)
		match = false
	} else {
		fmt.Printf("%sEntrypoint: %d ✓\n", indent, control.Graph.Entrypoint)
	}

	if control.Graph.Level != snapshot.Graph.Level {
		fmt.Printf("%sLevel: %d vs %d ❌\n", indent, control.Graph.Level, snapshot.Graph.Level)
		match = false
	} else {
		fmt.Printf("%sLevel: %d ✓\n", indent, control.Graph.Level)
	}

	// Verify NodesDeleted: these nodes MUST be nil in snapshot
	nodesDeletedCorrect := 0
	nodesDeletedWrong := 0
	for nodeID := range control.Graph.NodesDeleted {
		if nodeID < uint64(len(snapshot.Graph.Nodes)) && snapshot.Graph.Nodes[nodeID] != nil {
			nodesDeletedWrong++
		} else {
			nodesDeletedCorrect++
		}
	}
	if nodesDeletedWrong > 0 {
		fmt.Printf("%sDeleted nodes nil in snapshot: %d correct, %d wrong ❌\n", indent, nodesDeletedCorrect, nodesDeletedWrong)
		match = false
	} else {
		fmt.Printf("%sDeleted nodes nil in snapshot: %d ✓\n", indent, nodesDeletedCorrect)
	}

	// Verify TombstonesDeleted: these MUST NOT have tombstones in snapshot
	tombstonesDeletedCorrect := 0
	tombstonesDeletedWrong := 0
	for nodeID := range control.Graph.TombstonesDeleted {
		if _, hasTombstone := snapshot.Graph.Tombstones[nodeID]; hasTombstone {
			tombstonesDeletedWrong++
		} else {
			tombstonesDeletedCorrect++
		}
	}
	if tombstonesDeletedWrong > 0 {
		fmt.Printf("%sDeleted tombstones absent in snapshot: %d correct, %d wrong ❌\n", indent, tombstonesDeletedCorrect, tombstonesDeletedWrong)
		match = false
	} else {
		fmt.Printf("%sDeleted tombstones absent in snapshot: %d ✓\n", indent, tombstonesDeletedCorrect)
	}

	// Compute expected active tombstones
	expectedTombstones := make(map[uint64]struct{})
	for id := range control.Graph.Tombstones {
		if _, deleted := control.Graph.TombstonesDeleted[id]; !deleted {
			expectedTombstones[id] = struct{}{}
		}
	}

	// Verify tombstone counts
	if len(expectedTombstones) != len(snapshot.Graph.Tombstones) {
		fmt.Printf("%sActive tombstones: expected %d, got %d ❌\n", indent, len(expectedTombstones), len(snapshot.Graph.Tombstones))
		match = false
	} else {
		fmt.Printf("%sActive tombstones: %d ✓\n", indent, len(expectedTombstones))
	}

	// Compare nodes (only non-deleted ones)
	maxLen := len(control.Graph.Nodes)
	if len(snapshot.Graph.Nodes) > maxLen {
		maxLen = len(snapshot.Graph.Nodes)
	}

	nonNilMatched := 0
	nodeMismatches := 0
	linkMismatches := 0

	for i := 0; i < maxLen; i++ {
		var controlNode, snapshotNode *ent.Vertex

		if i < len(control.Graph.Nodes) {
			controlNode = control.Graph.Nodes[i]
		}
		if i < len(snapshot.Graph.Nodes) {
			snapshotNode = snapshot.Graph.Nodes[i]
		}

		// If node is deleted in control, expect nil in snapshot
		if controlNode != nil {
			if _, deleted := control.Graph.NodesDeleted[controlNode.ID]; deleted {
				controlNode = nil
			}
		}

		if controlNode == nil && snapshotNode == nil {
			continue
		}
		if controlNode == nil {
			nodeMismatches++
			continue
		}
		if snapshotNode == nil {
			nodeMismatches++
			continue
		}

		// Both non-nil - compare
		if controlNode.ID != snapshotNode.ID || controlNode.Level != snapshotNode.Level {
			nodeMismatches++
			continue
		}

		// Compare connections
		if !compareConnections(controlNode, snapshotNode) {
			linkMismatches++
			continue
		}

		nonNilMatched++
	}

	if nodeMismatches > 0 {
		fmt.Printf("%sNode mismatches: %d ❌\n", indent, nodeMismatches)
		match = false
	}

	if linkMismatches > 0 {
		fmt.Printf("%sLink mismatches: %d nodes ❌\n", indent, linkMismatches)
		match = false
	}

	if nodeMismatches == 0 && linkMismatches == 0 {
		fmt.Printf("%sNon-nil nodes matched: %d ✓\n", indent, nonNilMatched)
	}

	return match
}
