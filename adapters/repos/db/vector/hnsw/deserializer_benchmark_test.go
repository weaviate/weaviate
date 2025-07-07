//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
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
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)

	maxNodeID := uint64(1000000)
	r := rand.New(rand.NewSource(42))
	const M = 32
	commitLogs := 5000000
	connections := make([]int, maxNodeID)
	skipped := 0

	// Generate realistic level using HNSW probability (most at level 0)
	generateLevel := func() uint16 {
		level := 0
		for r.Float64() < 0.5 && level < 6 { // Cap at level 6 (realistic max)
			level++
		}
		return uint16(level)
	}

	// Generate realistic connection count based on level and HNSW limits
	generateConnectionCount := func(level uint16) uint16 {
		var maxConn int
		if level == 0 {
			maxConn = M * 2 // Allow some overflow during construction
		} else {
			maxConn = M
		}

		// Most connections are near the limit (realistic HNSW behavior)
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
				skipped += 1
				continue
			} else {
				connections[sourceID] += int(connCount)
			}

			writer.WriteByte(byte(commit))
			binary.Write(writer, binary.LittleEndian, sourceID)
			binary.Write(writer, binary.LittleEndian, level)
			binary.Write(writer, binary.LittleEndian, connCount)
			for j := 0; j < int(connCount); j++ {
				binary.Write(writer, binary.LittleEndian, uint64(r.Int63n(int64(maxNodeID))))
			}

		case AddNode:
			nodeID := uint64(r.Int63n(int64(maxNodeID)))
			level := generateLevel()
			writer.WriteByte(byte(commit))
			binary.Write(writer, binary.LittleEndian, nodeID)
			binary.Write(writer, binary.LittleEndian, level)

		case AddLinkAtLevel:
			sourceID := uint64(r.Int63n(int64(maxNodeID)))

			if connections[sourceID] > 2*M {
				skipped += 1
				continue
			} else {
				connections[sourceID] += 1
			}

			level := generateLevel()
			target := uint64(r.Int63n(int64(maxNodeID)))

			writer.WriteByte(byte(commit))
			binary.Write(writer, binary.LittleEndian, sourceID)
			binary.Write(writer, binary.LittleEndian, level)
			binary.Write(writer, binary.LittleEndian, target)

		case ClearLinksAtLevel:
			nodeID := uint64(r.Int63n(int64(maxNodeID)))
			connections[nodeID] = 0
			level := generateLevel()
			writer.WriteByte(byte(commit))
			binary.Write(writer, binary.LittleEndian, nodeID)
			binary.Write(writer, binary.LittleEndian, level)
		default:
			continue
		}
	}
	writer.Flush()

	// Create deserializer
	logger, _ := test.NewNullLogger()
	deserializer := NewDeserializer(logger)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(buf.Bytes()))
		_, _, err := deserializer.Do(reader, nil, true)
		if err != nil {
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
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)

	maxNodeID := uint64(1000000)
	r := rand.New(rand.NewSource(42))
	const linksPerOperation = 64
	commitLogs := 1000000 // 1M operations
	level := uint16(0)    // Always use level 0 for simiplicity

	// Track which nodes have links at level 0
	nodesWithLinks := make(map[uint64]bool)
	addLinksCount := 0
	clearLinksCount := 0

	for i := 0; i < commitLogs; i++ {
		sourceID := uint64(r.Int63n(int64(maxNodeID)))

		// Check if this node already has links at level 0
		if nodesWithLinks[sourceID] {
			// Clear existing links first
			writer.WriteByte(byte(ClearLinksAtLevel))
			binary.Write(writer, binary.LittleEndian, sourceID)
			binary.Write(writer, binary.LittleEndian, level)
			clearLinksCount++
			nodesWithLinks[sourceID] = false
		}

		// Add 64 random links
		targets := make([]uint64, linksPerOperation)
		for j := 0; j < linksPerOperation; j++ {
			targets[j] = uint64(r.Int63n(int64(maxNodeID)))
		}

		writer.WriteByte(byte(AddLinksAtLevel))
		binary.Write(writer, binary.LittleEndian, sourceID)
		binary.Write(writer, binary.LittleEndian, level)
		binary.Write(writer, binary.LittleEndian, uint16(linksPerOperation))
		for _, target := range targets {
			binary.Write(writer, binary.LittleEndian, target)
		}

		nodesWithLinks[sourceID] = true
		addLinksCount++
	}
	writer.Flush()

	// Create deserializer
	logger, _ := test.NewNullLogger()
	deserializer := NewDeserializer(logger)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(buf.Bytes()))
		_, _, err := deserializer.Do(reader, nil, true)
		if err != nil {
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
