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
	"io"
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

// e2e_compactor_validation performs end-to-end validation of the Compactor orchestrator:
// 1. Copies .condensed files from test_data.bak.d to a working directory (test_data)
// 2. Reads all .condensed files sequentially to compute the expected result (control)
// 3. Runs the Compactor in a loop until there's no more work to do
// 4. Reads the final compacted result
// 5. Compares the compacted result against the control to ensure correctness

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

	// Setup logger with debug level to see all compactor decisions
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	fmt.Println("=== Compactor V2 E2E Validation (Using Orchestrator) ===\n")

	// Paths
	backupDir := "../test_data.bak.d"
	workingDir := "../test_data"

	// Step 1: Setup working directory
	fmt.Println("Step 1: Setting up working directory...")
	startStep1 := time.Now()

	// Remove existing working directory if it exists
	if _, err := os.Stat(workingDir); err == nil {
		fmt.Printf("  Removing existing directory: %s\n", workingDir)
		if err := os.RemoveAll(workingDir); err != nil {
			panic(fmt.Errorf("remove existing working dir: %w", err))
		}
	}

	// Create fresh working directory
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		panic(fmt.Errorf("create working directory: %w", err))
	}

	// Copy .condensed files from backup to working directory
	fmt.Printf("  Copying .condensed files from %s to %s...\n", backupDir, workingDir)
	condensedFiles, err := filepath.Glob(filepath.Join(backupDir, "*.condensed"))
	if err != nil {
		panic(err)
	}
	sort.Strings(condensedFiles)

	if len(condensedFiles) == 0 {
		fmt.Printf("❌ No .condensed files found in %s\n", backupDir)
		os.Exit(1)
	}

	fmt.Printf("  Found %d .condensed files to copy\n", len(condensedFiles))
	for i, srcPath := range condensedFiles {
		filename := filepath.Base(srcPath)
		dstPath := filepath.Join(workingDir, filename)
		fmt.Printf("  [%d/%d] Copying %s...\n", i+1, len(condensedFiles), filename)
		if err := copyFile(srcPath, dstPath); err != nil {
			panic(fmt.Errorf("copy %s: %w", filename, err))
		}
	}

	step1Duration := time.Since(startStep1)
	fmt.Printf("\n✓ Step 1 completed in %v\n\n", step1Duration)

	// Step 2: Compute expected result (control) by reading all .condensed files sequentially
	fmt.Println("Step 2: Computing expected result (CONTROL) by reading all .condensed files...")
	startStep2 := time.Now()

	// Get list of .condensed files in working directory
	workingCondensedFiles, err := filepath.Glob(filepath.Join(workingDir, "*.condensed"))
	if err != nil {
		panic(err)
	}
	sort.Strings(workingCondensedFiles)

	var controlResult *ent.DeserializationResult
	for i, logPath := range workingCondensedFiles {
		fmt.Printf("  [%d/%d] %s\n", i+1, len(workingCondensedFiles), filepath.Base(logPath))
		logFile, _ := os.Open(logPath)
		logReader := bufio.NewReaderSize(logFile, 256*1024)
		walReader := compactv2.NewWALCommitReader(logReader, logger)
		memReader := compactv2.NewInMemoryReader(walReader, logger)
		controlResult, _ = memReader.Do(controlResult, true)
		logFile.Close()
	}
	step2Duration := time.Since(startStep2)

	fmt.Printf("\n✓ Control result (took %v):\n", step2Duration)
	fmt.Printf("  Nodes: %d\n", len(controlResult.Graph.Nodes))
	fmt.Printf("  Tombstones: %d\n", len(controlResult.Graph.Tombstones))
	fmt.Printf("  TombstonesDeleted: %d\n", len(controlResult.Graph.TombstonesDeleted))
	fmt.Printf("  NodesDeleted: %d\n", len(controlResult.Graph.NodesDeleted))
	fmt.Printf("  Entrypoint: %d (level %d)\n\n", controlResult.Graph.Entrypoint, controlResult.Graph.Level)

	// Step 3: Run the Compactor in a loop until no more work to do
	fmt.Println("Step 3: Running Compactor in a loop until convergence...")
	startStep3 := time.Now()

	config := compactv2.DefaultCompactorConfig(workingDir)
	compactor := compactv2.NewCompactor(config, logger)

	cycleCount := 0
	maxCycles := 20 // Safety limit to prevent infinite loops
	var cycleActions []compactv2.Action

	for cycleCount < maxCycles {
		cycleCount++
		fmt.Printf("\n--- Compactor Cycle %d ---\n", cycleCount)

		action, err := compactor.RunCycle()
		if err != nil {
			panic(fmt.Errorf("compactor cycle %d failed: %w", cycleCount, err))
		}

		cycleActions = append(cycleActions, action)
		fmt.Printf("Cycle %d action: %s\n", cycleCount, action.String())

		if action == compactv2.ActionNone {
			fmt.Printf("\n✓ Compactor converged after %d cycles (no more work to do)\n", cycleCount)
			break
		}
	}

	if cycleCount >= maxCycles {
		fmt.Printf("❌ WARNING: Compactor did not converge within %d cycles\n", maxCycles)
	}

	step3Duration := time.Since(startStep3)
	fmt.Printf("\n✓ Step 3 completed in %v\n", step3Duration)
	fmt.Printf("  Cycle summary: ")
	for i, action := range cycleActions {
		if i > 0 {
			fmt.Printf(" → ")
		}
		fmt.Printf("%s", action.String())
	}
	fmt.Printf("\n\n")

	// Step 4: Read the final compacted result
	fmt.Println("Step 4: Reading final compacted result...")
	startStep4 := time.Now()

	discovery := compactv2.NewFileDiscovery(workingDir)
	state, err := discovery.Scan()
	if err != nil {
		panic(fmt.Errorf("file discovery: %w", err))
	}

	var finalResult *ent.DeserializationResult

	if state.Snapshot != nil {
		// Read snapshot
		fmt.Printf("  Reading snapshot: %s\n", filepath.Base(state.Snapshot.Path))
		snapshotReader := compactv2.NewSnapshotReader(logger)
		finalResult, err = snapshotReader.ReadFromFile(state.Snapshot.Path)
		if err != nil {
			panic(fmt.Errorf("read snapshot: %w", err))
		}
	} else if len(state.SortedFiles) > 0 {
		// Read sorted files sequentially
		fmt.Printf("  Reading %d sorted file(s)\n", len(state.SortedFiles))
		for i, f := range state.SortedFiles {
			fmt.Printf("    [%d/%d] %s\n", i+1, len(state.SortedFiles), filepath.Base(f.Path))
			logFile, _ := os.Open(f.Path)
			logReader := bufio.NewReaderSize(logFile, 256*1024)
			walReader := compactv2.NewWALCommitReader(logReader, logger)
			memReader := compactv2.NewInMemoryReader(walReader, logger)
			finalResult, _ = memReader.Do(finalResult, true)
			logFile.Close()
		}
	} else {
		panic(fmt.Errorf("no snapshot or sorted files found after compaction"))
	}

	step4Duration := time.Since(startStep4)

	fmt.Printf("\n✓ Final result (took %v):\n", step4Duration)
	fmt.Printf("  Nodes: %d\n", len(finalResult.Graph.Nodes))
	fmt.Printf("  Tombstones: %d\n", len(finalResult.Graph.Tombstones))
	fmt.Printf("  TombstonesDeleted: %d\n", len(finalResult.Graph.TombstonesDeleted))
	fmt.Printf("  NodesDeleted: %d\n", len(finalResult.Graph.NodesDeleted))
	fmt.Printf("  Entrypoint: %d (level %d)\n\n", finalResult.Graph.Entrypoint, finalResult.Graph.Level)

	// Step 5: Compare control vs final result
	fmt.Println("Step 5: Comparing control vs compacted results...")
	startStep5 := time.Now()
	fmt.Println("─────────────────────────────────────────────────────────────")

	var allMatch bool
	if state.Snapshot != nil {
		fmt.Println("\n📊 Control (.condensed) vs Compacted (snapshot):")
		allMatch = compareResultsForSnapshot("  ", controlResult, finalResult)
	} else {
		fmt.Println("\n📊 Control (.condensed) vs Compacted (sorted):")
		allMatch = compareResults("  ", controlResult, finalResult)
	}

	step5Duration := time.Since(startStep5)
	totalDuration := time.Since(startTotal)

	fmt.Println("\n─────────────────────────────────────────────────────────────")
	fmt.Println("\n⏱️  Performance Summary:")
	fmt.Printf("  Step 1 (Setup & copy):           %v\n", step1Duration)
	fmt.Printf("  Step 2 (Read control):           %v\n", step2Duration)
	fmt.Printf("  Step 3 (Run compactor):          %v (%d cycles)\n", step3Duration, cycleCount)
	fmt.Printf("  Step 4 (Read final result):      %v\n", step4Duration)
	fmt.Printf("  Step 5 (Comparison):             %v\n", step5Duration)
	fmt.Printf("  ─────────────────────────────────────\n")
	fmt.Printf("  Total time:                      %v\n", totalDuration)

	fmt.Println("\n─────────────────────────────────────────────────────────────")
	if allMatch {
		fmt.Println("\n✅ SUCCESS: Compactor produced correct results!")
		fmt.Println("\n✓ The compactor correctly:")
		fmt.Println("  1. Orchestrated the conversion of .condensed files to .sorted format")
		fmt.Println("  2. Merged sorted files when needed")
		fmt.Println("  3. Created snapshots at the right time")
		fmt.Println("  4. Preserved all tombstone operations")
		fmt.Println("  5. Preserved all node connections/links")
		fmt.Println("  6. Produced identical results to sequential processing")
	} else {
		fmt.Println("\n❌ FAILURE: Results do not match!")
		os.Exit(1)
	}
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
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
					fmt.Printf("%s  Expected:\n", indent)
					iterA := nodeA.Connections.Iterator()
					for iterA.Next() {
						level, links := iterA.Current()
						fmt.Printf("%s    Level %d: %d links: %v\n", indent, level, len(links), links)
					}
				} else {
					fmt.Printf("%s  Expected connections: nil\n", indent)
				}

				if nodeB.Connections != nil {
					fmt.Printf("%s  Actual:\n", indent)
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
