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
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

// Vertex represents an HNSW node in its serialized/deserialized form.
// This is a data-only structure without synchronization primitives,
// suitable for serialization and read-only operations.
type Vertex struct {
	ID          uint64
	Level       int
	Connections *packedconn.Connections
}

// DeserializationResult holds the complete in-memory state after deserializing
// HNSW commit logs or snapshots.
//
// This structure is designed to be shared between different HNSW implementations
// (e.g., the main hnsw adapter and compactv2) to enable interoperability and
// migration between formats.
//
// It composes GraphState (graph structure) and compression.State (optional compression data)
// to maintain clean separation of concerns.
type DeserializationResult struct {
	// Graph contains the HNSW graph structure.
	Graph *GraphState

	// Compression contains optional compression/encoding data.
	// This is nil if the index is uncompressed.
	Compression *compression.State
}

// HasCompression returns true if compression is enabled for this result.
func (dr *DeserializationResult) HasCompression() bool {
	return dr.Compression != nil && dr.Compression.Compressed
}

// HasMuvera returns true if Muvera encoding is enabled for this result.
func (dr *DeserializationResult) HasMuvera() bool {
	return dr.Compression != nil && dr.Compression.MuveraEnabled
}

// ReplaceLinks checks if links at the given node and level should be replaced.
// Delegates to GraphState.ReplaceLinks.
func (dr *DeserializationResult) ReplaceLinks(node uint64, level uint16) bool {
	if dr == nil || dr.Graph == nil {
		return false
	}
	return dr.Graph.ReplaceLinks(node, level)
}

// NewDeserializationResult creates a new DeserializationResult with initialized graph state.
func NewDeserializationResult(initialCapacity int) *DeserializationResult {
	return &DeserializationResult{
		Graph: NewGraphState(initialCapacity),
	}
}

// IsEmpty returns true if no meaningful data has been loaded into this result.
// A result is considered empty if all nodes are nil, no entrypoint has been set,
// and there's no compression data.
func (dr *DeserializationResult) IsEmpty() bool {
	if dr == nil || dr.Graph == nil {
		return true
	}

	// Check if entrypoint was explicitly set (EntrypointChanged tracks this)
	if dr.Graph.EntrypointChanged {
		return false
	}

	// Check for any compression data
	if dr.Compression != nil {
		return false
	}

	// Check if any nodes have data
	for _, node := range dr.Graph.Nodes {
		if node != nil {
			return false
		}
	}

	// Check for any tombstones
	if len(dr.Graph.Tombstones) > 0 {
		return false
	}

	return true
}

// Convenience accessors for backward compatibility.
// These provide direct access to frequently-used GraphState fields.

// Nodes returns the graph nodes slice.
func (dr *DeserializationResult) Nodes() []*Vertex {
	if dr == nil || dr.Graph == nil {
		return nil
	}
	return dr.Graph.Nodes
}

// SetNodes sets the graph nodes slice.
func (dr *DeserializationResult) SetNodes(nodes []*Vertex) {
	if dr != nil && dr.Graph != nil {
		dr.Graph.Nodes = nodes
	}
}

// NodesDeleted returns the deleted nodes map.
func (dr *DeserializationResult) NodesDeleted() map[uint64]struct{} {
	if dr == nil || dr.Graph == nil {
		return nil
	}
	return dr.Graph.NodesDeleted
}

// Entrypoint returns the graph entrypoint.
func (dr *DeserializationResult) Entrypoint() uint64 {
	if dr == nil || dr.Graph == nil {
		return 0
	}
	return dr.Graph.Entrypoint
}

// SetEntrypoint sets the graph entrypoint.
func (dr *DeserializationResult) SetEntrypoint(ep uint64) {
	if dr != nil && dr.Graph != nil {
		dr.Graph.Entrypoint = ep
	}
}

// Level returns the graph maximum level.
func (dr *DeserializationResult) Level() uint16 {
	if dr == nil || dr.Graph == nil {
		return 0
	}
	return dr.Graph.Level
}

// SetLevel sets the graph maximum level.
func (dr *DeserializationResult) SetLevel(level uint16) {
	if dr != nil && dr.Graph != nil {
		dr.Graph.Level = level
	}
}

// Tombstones returns the tombstones map.
func (dr *DeserializationResult) Tombstones() map[uint64]struct{} {
	if dr == nil || dr.Graph == nil {
		return nil
	}
	return dr.Graph.Tombstones
}

// TombstonesDeleted returns the deleted tombstones map.
func (dr *DeserializationResult) TombstonesDeleted() map[uint64]struct{} {
	if dr == nil || dr.Graph == nil {
		return nil
	}
	return dr.Graph.TombstonesDeleted
}

// EntrypointChanged returns whether the entrypoint was changed.
func (dr *DeserializationResult) EntrypointChanged() bool {
	if dr == nil || dr.Graph == nil {
		return false
	}
	return dr.Graph.EntrypointChanged
}

// SetEntrypointChanged sets whether the entrypoint was changed.
func (dr *DeserializationResult) SetEntrypointChanged(changed bool) {
	if dr != nil && dr.Graph != nil {
		dr.Graph.EntrypointChanged = changed
	}
}

// LinksReplaced returns the links replaced map.
func (dr *DeserializationResult) LinksReplaced() map[uint64]map[uint16]struct{} {
	if dr == nil || dr.Graph == nil {
		return nil
	}
	return dr.Graph.LinksReplaced
}

// Compression data accessors for backward compatibility.

// CompressionPQData returns the PQ compression data.
func (dr *DeserializationResult) CompressionPQData() *PQData {
	if dr == nil || dr.Compression == nil {
		return nil
	}
	return dr.Compression.PQData
}

// SetCompressionPQData sets the PQ compression data.
func (dr *DeserializationResult) SetCompressionPQData(data *PQData) {
	if dr != nil {
		dr.ensureCompression()
		dr.Compression.PQData = data
	}
}

// CompressionSQData returns the SQ compression data.
func (dr *DeserializationResult) CompressionSQData() *SQData {
	if dr == nil || dr.Compression == nil {
		return nil
	}
	return dr.Compression.SQData
}

// SetCompressionSQData sets the SQ compression data.
func (dr *DeserializationResult) SetCompressionSQData(data *SQData) {
	if dr != nil {
		dr.ensureCompression()
		dr.Compression.SQData = data
	}
}

// CompressionRQData returns the RQ compression data.
func (dr *DeserializationResult) CompressionRQData() *RQData {
	if dr == nil || dr.Compression == nil {
		return nil
	}
	return dr.Compression.RQData
}

// SetCompressionRQData sets the RQ compression data.
func (dr *DeserializationResult) SetCompressionRQData(data *RQData) {
	if dr != nil {
		dr.ensureCompression()
		dr.Compression.RQData = data
	}
}

// CompressionBRQData returns the BRQ compression data.
func (dr *DeserializationResult) CompressionBRQData() *BRQData {
	if dr == nil || dr.Compression == nil {
		return nil
	}
	return dr.Compression.BRQData
}

// SetCompressionBRQData sets the BRQ compression data.
func (dr *DeserializationResult) SetCompressionBRQData(data *BRQData) {
	if dr != nil {
		dr.ensureCompression()
		dr.Compression.BRQData = data
	}
}

// MuveraEnabled returns whether Muvera encoding is enabled.
func (dr *DeserializationResult) MuveraEnabled() bool {
	if dr == nil || dr.Compression == nil {
		return false
	}
	return dr.Compression.MuveraEnabled
}

// SetMuveraEnabled sets whether Muvera encoding is enabled.
func (dr *DeserializationResult) SetMuveraEnabled(enabled bool) {
	if dr != nil {
		dr.ensureCompression()
		dr.Compression.MuveraEnabled = enabled
	}
}

// EncoderMuvera returns the Muvera encoder data.
func (dr *DeserializationResult) EncoderMuvera() *MuveraData {
	if dr == nil || dr.Compression == nil {
		return nil
	}
	return dr.Compression.MuveraData
}

// SetEncoderMuvera sets the Muvera encoder data.
func (dr *DeserializationResult) SetEncoderMuvera(data *MuveraData) {
	if dr != nil {
		dr.ensureCompression()
		dr.Compression.MuveraData = data
	}
}

// Compressed returns whether compression is enabled.
func (dr *DeserializationResult) Compressed() bool {
	if dr == nil || dr.Compression == nil {
		return false
	}
	return dr.Compression.Compressed
}

// SetCompressed sets whether compression is enabled.
func (dr *DeserializationResult) SetCompressed(compressed bool) {
	if dr != nil {
		dr.ensureCompression()
		dr.Compression.Compressed = compressed
	}
}

// ensureCompression ensures the Compression field is initialized.
func (dr *DeserializationResult) ensureCompression() {
	if dr.Compression == nil {
		dr.Compression = &compression.State{}
	}
}
