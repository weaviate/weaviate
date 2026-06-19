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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// TestSnapshotRoundTrip_MultiBlock exercises the block-spanning streaming path
// (writeSlot/initBlock/flushBlock) end-to-end: it writes a graph that spans
// several body blocks, with interior gaps, a multi-level node, per-node
// tombstones, and a trailing standalone tombstone, then reads it back and
// asserts every node, connection layer, nil slot, and tombstone survives.
// TestSnapshotWriter_SmallBlockSize only checks the writer does not crash; this
// is the read-back assertion for the highest-risk new code.
func TestSnapshotRoundTrip_MultiBlock(t *testing.T) {
	const blockSize = 128 // small blocks force many splits

	type node struct {
		id    uint64
		level uint16
		conns [][]uint64
		tomb  bool
	}

	var nodes []node
	for i := uint64(0); i < 40; i++ {
		if i == 4 || i == 13 || i == 27 {
			continue // interior gaps (nil slots)
		}
		conns := [][]uint64{{(i + 1) % 40, (i + 2) % 40, (i + 3) % 40}}
		level := uint16(0)
		if i == 9 || i == 30 {
			conns = append(conns, []uint64{i}) // a couple of level-1 nodes
			level = 1
		}
		nodes = append(nodes, node{id: i, level: level, conns: conns, tomb: i%6 == 1})
	}

	var buf bytes.Buffer
	sw := NewSnapshotWriterWithBlockSize(&buf, blockSize)
	sw.SetEntrypoint(9, 1) // node 9 is live with level 1
	for _, n := range nodes {
		sw.AddNode(n.id, n.level, n.conns, n.tomb)
	}
	sw.AddTombstone(55) // trailing standalone tombstone past the last live node
	require.NoError(t, sw.Flush())

	// Confirm the body really did span >= 3 blocks, otherwise the test would
	// not exercise the cross-block path it is meant to cover.
	metadataSize := binary.LittleEndian.Uint32(buf.Bytes()[5:9])
	bodyStart := 9 + int(metadataSize)
	require.GreaterOrEqual(t, (buf.Len()-bodyStart)/blockSize, 3,
		"test must span >=3 blocks to exercise the cross-block path")

	// The reader steps through the body at its configured block size, so it
	// must match the writer's (in production both use defaultBlockSize).
	result, err := NewSnapshotReaderWithBlockSize(logrus.New(), blockSize).Read(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	assert.Equal(t, uint64(9), result.Graph.Entrypoint)
	assert.Equal(t, uint16(1), result.Graph.Level)
	require.Equal(t, 56, len(result.Graph.Nodes)) // 0..55 (trailing tombstone extends to 55)

	live := make(map[uint64]node, len(nodes))
	for _, n := range nodes {
		live[n.id] = n
	}

	for id := uint64(0); id < 56; id++ {
		n, ok := live[id]
		got := result.Graph.Nodes[id]
		if !ok {
			assert.Nil(t, got, "slot %d must be nil", id)
			_, hasTomb := result.Graph.Tombstones[id]
			assert.False(t, hasTomb, "nil slot %d must not carry a tombstone", id)
			continue
		}
		require.NotNil(t, got, "node %d must survive round-trip", id)
		assert.Equal(t, int(n.level), got.Level, "node %d level", id)
		for lvl, conns := range n.conns {
			assert.Equal(t, conns, got.Connections.GetLayer(uint8(lvl)),
				"node %d level %d connections", id, lvl)
		}
		_, hasTomb := result.Graph.Tombstones[id]
		assert.Equal(t, n.tomb, hasTomb, "node %d tombstone", id)
	}
}

// goldenSnapshots are the V3 on-disk byte streams (sha256) for a battery of
// deterministic graphs at blockSize 256. They pin the format: any drift in the
// body block layout or the metadata/compression serialization flips a hash.
//
// To regenerate after an *intentional* format change: run this test, copy the
// "GOLDEN <name> sha256=<hash>" lines printed on failure into the map below.
var goldenSnapshots = map[string]string{
	"dense_gaps_tomb": "762a4d96dd579826ca1d1b1756d6b5a8798f3ef808161cf8f6911392dbb3dbab",
	"multi_level":     "3a1fb686f45cd9b772fd1a5e97f0c9c4df62cce3586b6ebb888e67e63a70cfe6",
	"sq":              "e2fd7d6e337f5abc66c1f77958579aa953a07b2523ae063e229999eacfec6c4c",
	"rq":              "23d25e01a3df73db20d4a42d3b9854095550b0ade83237d88bec714d6f4ab056",
	"brq":             "25a14da56a156e123f6b98730f4fdd06c2265e15ee82df8e198715f651bf9865",
	"muvera":          "1cdc53f8872dbe22649c6d71f6bb09e070188e43b7cb5649305ded0760632027",
}

// TestSnapshotWriter_ByteIdentityGolden pins the V3 on-disk format byte-for-byte
// across gaps, tombstones, multi-block splits, trailing tombstones, and every
// compression/encoder type, so a future change cannot silently alter the format.
func TestSnapshotWriter_ByteIdentityGolden(t *testing.T) {
	cases := map[string]func(*SnapshotWriter){
		"dense_gaps_tomb": func(sw *SnapshotWriter) {
			sw.SetEntrypoint(7, 3)
			for i := uint64(0); i < 40; i++ {
				if i%5 == 0 {
					continue
				}
				sw.AddNode(i, 0, [][]uint64{{(i + 1) % 40, (i + 2) % 40, (i + 3) % 40}}, i%7 == 0)
			}
			sw.AddTombstone(60)
		},
		"multi_level": func(sw *SnapshotWriter) {
			sw.SetEntrypoint(0, 2)
			sw.AddNode(0, 2, [][]uint64{{1, 2, 3, 4, 5}, {10, 20, 30}, {100, 200}}, false)
		},
		"sq": func(sw *SnapshotWriter) {
			sw.SetEntrypoint(0, 0)
			sw.SetSQData(&compression.SQData{A: 0.5, B: 1.5, Dimensions: 128})
			sw.AddNode(0, 0, [][]uint64{{1, 2}}, false)
			sw.AddNode(2, 0, [][]uint64{{0}}, true)
		},
		"rq": func(sw *SnapshotWriter) {
			sw.SetEntrypoint(0, 0)
			sw.SetRQData(&compression.RQData{
				InputDim: 128, Bits: 4,
				Rotation: compression.FastRotation{
					OutputDim: 4, Rounds: 2,
					Swaps: [][]compression.Swap{{{I: 0, J: 1}, {I: 2, J: 3}}, {{I: 0, J: 1}, {I: 2, J: 3}}},
					Signs: [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}},
				},
			})
			sw.AddNode(0, 0, [][]uint64{{1}}, false)
		},
		"brq": func(sw *SnapshotWriter) {
			sw.SetEntrypoint(0, 0)
			sw.SetBRQData(&compression.BRQData{
				InputDim: 128,
				Rotation: compression.FastRotation{
					OutputDim: 4, Rounds: 2,
					Swaps: [][]compression.Swap{{{I: 0, J: 1}, {I: 2, J: 3}}, {{I: 0, J: 1}, {I: 2, J: 3}}},
					Signs: [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}},
				},
				Rounding: []float32{1, 2, 3, 4},
			})
			sw.AddNode(0, 0, [][]uint64{{1}}, false)
		},
		"muvera": func(sw *SnapshotWriter) {
			sw.SetEntrypoint(0, 0)
			sw.SetMuveraData(&multivector.MuveraData{
				Dimensions: 2, KSim: 1, NumClusters: 2, DProjections: 1, Repetitions: 1,
				Gaussians: [][][]float32{{{0.1, 0.2}}},
				S:         [][][]float32{{{0.3, 0.4}}},
			})
			sw.AddNode(0, 0, [][]uint64{{1}}, false)
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			sw := NewSnapshotWriterWithBlockSize(&buf, 256)
			build(sw)
			require.NoError(t, sw.Flush())

			sum := sha256.Sum256(buf.Bytes())
			gotHex := hex.EncodeToString(sum[:])
			if gotHex != goldenSnapshots[name] {
				t.Fatalf("GOLDEN %s sha256=%s (format drift: expected %s)", name, gotHex, goldenSnapshots[name])
			}
		})
	}
}
