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

package hnsw

import (
	"context"
	"sync"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

// BenchGraphStats reports the exact in-memory footprint of the graph
// structure, split from vector storage. Bench-only: takes the global read
// lock and walks every node, so it is not meant for production telemetry.
type BenchGraphStats struct {
	Nodes           uint64 // non-nil vertices
	Edges           uint64 // sum of connections over all nodes and layers
	ConnectionBytes uint64 // packed connection payload bytes (packedconn data)
	VertexOverhead  uint64 // vertex structs + connection struct headers + node slice slots
}

func (s BenchGraphStats) TotalBytes() uint64 {
	return s.ConnectionBytes + s.VertexOverhead
}

// BenchGraphStats walks the graph under the read lock and returns exact edge
// counts and packed connection bytes.
func (h *hnsw) BenchGraphStats() BenchGraphStats {
	h.RLock()
	defer h.RUnlock()

	var stats BenchGraphStats
	// Per-slot cost of the nodes slice (one pointer per allocated slot) plus
	// per-vertex struct costs from the actual type layouts. Connection
	// payload bytes are recovered from the serialized form: Data() frames
	// the payload with 1 byte of layer count and 6 bytes per layer.
	const ptrSize = 8
	vertexStructSize := uint64(unsafe.Sizeof(vertex{}))
	connectionsStructSize := uint64(unsafe.Sizeof(packedconn.Connections{}))
	layerDataSize := uint64(unsafe.Sizeof(packedconn.LayerData{}))

	stats.VertexOverhead += uint64(cap(h.nodes)) * ptrSize
	for _, node := range h.nodes {
		if node == nil {
			continue
		}
		stats.Nodes++
		stats.VertexOverhead += vertexStructSize
		if node.connections == nil {
			continue
		}
		layers := uint64(node.connections.Layers())
		stats.VertexOverhead += connectionsStructSize + layers*layerDataSize
		serialized := uint64(len(node.connections.Data()))
		if framing := 1 + 6*layers; serialized > framing {
			stats.ConnectionBytes += serialized - framing
		}
		for layer := uint8(0); layer < node.connections.Layers(); layer++ {
			stats.Edges += uint64(node.connections.LenAtLayer(layer))
		}
	}
	return stats
}

// BenchSwapRQCompressor re-encodes every stored vector with a fresh RQ
// compressor built from opts and swaps it in, leaving the graph untouched.
// This is the build-full/query-truncated diagnostic: the graph keeps the
// shape it acquired at build width while queries run on codes of a different
// width. Bench-only; the caller must ensure the index is idle.
func (h *hnsw) BenchSwapRQCompressor(ctx context.Context, opts compressionhelpers.RQOptions) error {
	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()

	if !h.compressed.Load() || !h.rqActive.Load() {
		return errors.New("index is not RQ-compressed")
	}
	if h.multivector.Load() && !h.muvera.Load() {
		return errors.New("multi-vector indexes are not supported")
	}

	newCompressor, err := compressionhelpers.NewRQCompressor(
		h.distancerProvider, 1e12, h.logger, h.store, h.allocChecker, h.makeBucketOptions,
		int(h.rqConfig.Bits), int(h.dims.Load()), opts, h.getTargetVector(), h.vectorForID)
	if err != nil {
		return err
	}

	h.RLock()
	maxID := uint64(len(h.nodes))
	h.RUnlock()

	var errLock sync.Mutex
	var encodeErr error
	compressionhelpers.Concurrently(h.logger, maxID, func(id uint64) {
		h.shardedNodeLocks.RLock(id)
		node := h.nodes[id]
		h.shardedNodeLocks.RUnlock(id)
		if node == nil {
			return
		}
		vec, err := h.vectorForID(ctx, id)
		if err != nil {
			errLock.Lock()
			encodeErr = errors.Wrapf(err, "vector for id %d", id)
			errLock.Unlock()
			return
		}
		if h.distancerProvider.Type() == "cosine-dot" {
			vec = h.normalizeVec(vec)
		}
		newCompressor.Preload(id, vec)
	})
	if encodeErr != nil {
		return encodeErr
	}

	h.Lock()
	old := h.compressor
	h.compressor = newCompressor
	h.Unlock()
	// Drop only the old cache; the LSM bucket is shared and was rewritten by
	// Preload above.
	old.Drop()
	return nil
}
