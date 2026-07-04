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

package compact

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func sampleCommitType(r *rand.Rand) HnswCommitType {
	sample := r.Float64()
	if sample < 0.75 {
		return AddLinkAtLevel
	} else if sample < 0.95 {
		return ReplaceLinksAtLevel
	} else if sample < 0.97 {
		return AddNode
	} else {
		return ClearLinksAtLevel
	}
}

func BenchmarkDeserializerPerf(b *testing.B) {
	var buf bytes.Buffer
	writer := NewWALWriter(&buf)

	maxNodeID := uint64(1000000)
	r := rand.New(rand.NewSource(42))
	const M = 32
	commitLogs := 5000000
	connections := make([]int, maxNodeID)
	skipped := 0

	generateLevel := func() uint16 {
		level := 0
		for r.Float64() < 0.5 && level < 6 {
			level++
		}
		return uint16(level)
	}

	generateConnectionCount := func(level uint16) uint16 {
		var maxConn int
		if level == 0 {
			maxConn = M * 2
		} else {
			maxConn = M
		}
		minConn := maxConn / 2
		return uint16(r.Intn(maxConn-minConn+1) + minConn)
	}

	for i := 0; i < commitLogs; i++ {
		commit := sampleCommitType(r)

		switch commit {
		case ReplaceLinksAtLevel:
			sourceID := uint64(r.Int63n(int64(maxNodeID)))
			level := generateLevel()
			connCount := generateConnectionCount(level)

			if connections[sourceID] > 2*M {
				skipped++
				continue
			}
			connections[sourceID] += int(connCount)

			targets := make([]uint64, connCount)
			for j := range targets {
				targets[j] = uint64(r.Int63n(int64(maxNodeID)))
			}
			if err := writer.WriteReplaceLinksAtLevel(sourceID, level, targets); err != nil {
				b.Fatal(err)
			}

		case AddNode:
			nodeID := uint64(r.Int63n(int64(maxNodeID)))
			level := generateLevel()
			if err := writer.WriteAddNode(nodeID, level); err != nil {
				b.Fatal(err)
			}

		case AddLinkAtLevel:
			sourceID := uint64(r.Int63n(int64(maxNodeID)))

			if connections[sourceID] > 2*M {
				skipped++
				continue
			}
			connections[sourceID]++

			level := generateLevel()
			target := uint64(r.Int63n(int64(maxNodeID)))
			if err := writer.WriteAddLinkAtLevel(sourceID, level, target); err != nil {
				b.Fatal(err)
			}

		case ClearLinksAtLevel:
			nodeID := uint64(r.Int63n(int64(maxNodeID)))
			connections[nodeID] = 0
			level := generateLevel()
			if err := writer.WriteClearLinksAtLevel(nodeID, level); err != nil {
				b.Fatal(err)
			}

		default:
			continue
		}
	}

	logger := testLogger()
	data := buf.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		walReader := NewWALCommitReader(bytes.NewReader(data), logger)
		memReader := NewInMemoryReader(walReader, logger)
		if _, err := memReader.Do(nil, true); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(commitLogs), "commits/op")
	b.ReportMetric(float64(skipped), "skipped")
	nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
	commitsPerSecond := float64(commitLogs) * float64(time.Second.Nanoseconds()) / nsPerOp
	b.ReportMetric(commitsPerSecond, "commits/sec")
}

func BenchmarkAddLinksAtLevelPerf(b *testing.B) {
	var buf bytes.Buffer
	writer := NewWALWriter(&buf)

	maxNodeID := uint64(1000000)
	r := rand.New(rand.NewSource(42))
	const linksPerOperation = 64
	commitLogs := 1000000
	level := uint16(0)

	nodesWithLinks := make(map[uint64]bool)
	addLinksCount := 0
	clearLinksCount := 0

	for i := 0; i < commitLogs; i++ {
		sourceID := uint64(r.Int63n(int64(maxNodeID)))

		if nodesWithLinks[sourceID] {
			if err := writer.WriteClearLinksAtLevel(sourceID, level); err != nil {
				b.Fatal(err)
			}
			clearLinksCount++
			nodesWithLinks[sourceID] = false
		}

		targets := make([]uint64, linksPerOperation)
		for j := range targets {
			targets[j] = uint64(r.Int63n(int64(maxNodeID)))
		}

		if err := writer.WriteAddLinksAtLevel(sourceID, level, targets); err != nil {
			b.Fatal(err)
		}

		nodesWithLinks[sourceID] = true
		addLinksCount++
	}

	logger := testLogger()
	data := buf.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		walReader := NewWALCommitReader(bytes.NewReader(data), logger)
		memReader := NewInMemoryReader(walReader, logger)
		if _, err := memReader.Do(nil, true); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(commitLogs), "operations/op")
	b.ReportMetric(float64(addLinksCount), "add_links_operations")
	b.ReportMetric(float64(clearLinksCount), "clear_links_operations")
	b.ReportMetric(float64(linksPerOperation), "links_per_operation")
	nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
	operationsPerSecond := float64(commitLogs) * float64(time.Second.Nanoseconds()) / nsPerOp
	b.ReportMetric(operationsPerSecond, "operations/sec")
}
