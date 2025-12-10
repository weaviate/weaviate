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

package compactv2

import (
	"io"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
)

const (
	maxConnectionsPerNodeInMemory = 4096
	indexGrowthRate               = 1.2
)

// Vertex represents an HNSW node in memory.
// This is a simplified version without locking, suitable for read-only operations.
type Vertex struct {
	ID          uint64
	Level       int
	Connections *packedconn.Connections
}

// DeserializationResult holds the complete in-memory state after deserializing
// commit logs. This matches the structure from the v1 deserializer to enable
// migration from .condensed to .sorted files.
type DeserializationResult struct {
	Nodes              []*Vertex
	NodesDeleted       map[uint64]struct{}
	Entrypoint         uint64
	Level              uint16
	Tombstones         map[uint64]struct{}
	TombstonesDeleted  map[uint64]struct{}
	EntrypointChanged  bool
	CompressionPQData  *compressionhelpers.PQData
	CompressionSQData  *compressionhelpers.SQData
	CompressionRQData  *compressionhelpers.RQData
	CompressionBRQData *compressionhelpers.BRQData
	MuveraEnabled      bool
	EncoderMuvera      *multivector.MuveraData
	Compressed         bool

	// LinksReplaced tracks which nodes and levels have had their links replaced
	// rather than appended. This is critical for correctly handling .condensed
	// files, as described in https://github.com/weaviate/weaviate/issues/1868.
	//
	// If there is no entry for a node/level, we must assume that all links were
	// appended and prior state must exist. When we encounter a ReplaceLinksAtLevel
	// or ClearLinksAtLevel operation, we explicitly set the replace flag so that
	// future operations (including condensing multiple logs) correctly interpret
	// whether links should be replaced or appended.
	LinksReplaced map[uint64]map[uint16]struct{}
}

// ReplaceLinks checks if links at the given node and level should be replaced.
func (dr *DeserializationResult) ReplaceLinks(node uint64, level uint16) bool {
	levels, ok := dr.LinksReplaced[node]
	if !ok {
		return false
	}

	_, ok = levels[level]
	return ok
}

// InMemoryReader deserializes commit logs into an in-memory HNSW graph state.
// It uses the WALCommitReader to read commits and applies them to build up
// the complete graph structure.
//
// InMemoryReader is almost an exact copy of the v1 Deserializer. However, it
// is compatible with the v2 WALCommitReader instead of parsing a raw file like
// the old condensor did.
type InMemoryReader struct {
	logger logrus.FieldLogger
	reader *WALCommitReader
}

// NewInMemoryReader creates a new reader that deserializes commits into memory.
func NewInMemoryReader(reader *WALCommitReader, logger logrus.FieldLogger) *InMemoryReader {
	return &InMemoryReader{
		logger: logger,
		reader: reader,
	}
}

// Do reads all commits and builds the in-memory state.
// If initialState is provided, commits are applied on top of it.
// keepLinkReplaceInformation controls whether LinksReplaced tracking is maintained.
func (r *InMemoryReader) Do(initialState *DeserializationResult, keepLinkReplaceInformation bool) (*DeserializationResult, error) {
	out := initialState
	commitTypeMetrics := make(map[HnswCommitType]int)

	if out == nil {
		out = &DeserializationResult{
			Nodes:             make([]*Vertex, cache.InitialSize),
			NodesDeleted:      make(map[uint64]struct{}),
			Tombstones:        make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		}
	}

	for {
		c, err := r.reader.ReadNextCommit()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return out, err
		}

		commitTypeMetrics[c.Type()]++

		switch commit := c.(type) {
		case *AddNodeCommit:
			err = r.readNode(commit, out)
		case *SetEntryPointMaxLevelCommit:
			out.Entrypoint = commit.Entrypoint
			out.Level = commit.Level
			out.EntrypointChanged = true
		case *AddLinkAtLevelCommit:
			err = r.readLink(commit, out)
		case *AddLinksAtLevelCommit:
			err = r.readAddLinks(commit, out)
		case *ReplaceLinksAtLevelCommit:
			err = r.readReplaceLinks(commit, out, keepLinkReplaceInformation)
		case *AddTombstoneCommit:
			out.Tombstones[commit.ID] = struct{}{}
		case *RemoveTombstoneCommit:
			_, ok := out.Tombstones[commit.ID]
			if !ok {
				// Tombstone is not present but may exist in older commit log
				// We need to keep track of it so we can delete it later
				out.TombstonesDeleted[commit.ID] = struct{}{}
			} else {
				// Tombstone is present, we can delete it
				delete(out.Tombstones, commit.ID)
			}
		case *ClearLinksCommit:
			err = r.readClearLinks(commit, out, keepLinkReplaceInformation)
		case *ClearLinksAtLevelCommit:
			err = r.readClearLinksAtLevel(commit, out, keepLinkReplaceInformation)
		case *DeleteNodeCommit:
			err = r.readDeleteNode(commit, out)
		case *ResetIndexCommit:
			out.Entrypoint = 0
			out.Level = 0
			out.Nodes = make([]*Vertex, cache.InitialSize)
			out.Tombstones = make(map[uint64]struct{})
		case *AddPQCommit:
			out.CompressionPQData = commit.Data
			out.Compressed = true
		case *AddSQCommit:
			out.CompressionSQData = commit.Data
			out.Compressed = true
		case *AddRQCommit:
			out.CompressionRQData = commit.Data
			out.Compressed = true
		case *AddBRQCommit:
			out.CompressionBRQData = commit.Data
			out.Compressed = true
		case *AddMuveraCommit:
			out.EncoderMuvera = commit.Data
			out.MuveraEnabled = true
		default:
			err = errors.Errorf("unrecognized commit type %T", c)
		}

		if err != nil {
			return out, err
		}
	}

	for commitType, count := range commitTypeMetrics {
		r.logger.WithFields(logrus.Fields{
			"action": "hnsw_deserialization",
			"ops":    count,
		}).Debugf("hnsw commit logger %s", commitType)
	}

	return out, nil
}

func (r *InMemoryReader) readNode(c *AddNodeCommit, res *DeserializationResult) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Nodes, c.ID, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[c.ID] == nil {
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Nodes[c.ID] = &Vertex{
			Level:       int(c.Level),
			ID:          c.ID,
			Connections: conns,
		}
	} else {
		if res.Nodes[c.ID].Connections == nil {
			res.Nodes[c.ID].Connections, err = packedconn.NewWithMaxLayer(uint8(c.Level))
			if err != nil {
				return err
			}
		} else {
			res.Nodes[c.ID].Connections.GrowLayersTo(uint8(c.Level))
		}
		res.Nodes[c.ID].Level = int(c.Level)
	}
	return nil
}

func (r *InMemoryReader) readLink(c *AddLinkAtLevelCommit, res *DeserializationResult) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Nodes, c.Source, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[c.Source] == nil {
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Nodes[c.Source] = &Vertex{
			ID:          c.Source,
			Connections: conns,
		}
	}

	if res.Nodes[c.Source].Connections == nil {
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Nodes[c.Source].Connections = conns
	} else {
		res.Nodes[c.Source].Connections.GrowLayersTo(uint8(c.Level))
	}

	res.Nodes[c.Source].Connections.InsertAtLayer(c.Target, uint8(c.Level))
	return nil
}

func (r *InMemoryReader) readAddLinks(c *AddLinksAtLevelCommit, res *DeserializationResult) error {
	targets := c.Targets
	if len(targets) >= maxConnectionsPerNodeInMemory {
		r.logger.Warnf("read AddLinksAtLevel with %v (>= %d) connections for node %d at level %d, truncating to %d",
			len(targets), maxConnectionsPerNodeInMemory, c.Source, c.Level, maxConnectionsPerNodeInMemory)
		targets = targets[:maxConnectionsPerNodeInMemory]
	}

	newNodes, changed, err := growIndexToAccommodateNode(res.Nodes, c.Source, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[c.Source] == nil {
		res.Nodes[c.Source] = &Vertex{ID: c.Source}
	}

	if res.Nodes[c.Source].Connections == nil {
		res.Nodes[c.Source].Connections = &packedconn.Connections{}
	} else {
		res.Nodes[c.Source].Connections.GrowLayersTo(uint8(c.Level))
	}

	res.Nodes[c.Source].Connections.BulkInsertAtLayer(targets, uint8(c.Level))
	return nil
}

func (r *InMemoryReader) readReplaceLinks(c *ReplaceLinksAtLevelCommit, res *DeserializationResult, keepReplaceInfo bool) error {
	targets := c.Targets
	if len(targets) >= maxConnectionsPerNodeInMemory {
		r.logger.Warnf("read ReplaceLinksAtLevel with %v (>= %d) connections for node %d at level %d, truncating to %d",
			len(targets), maxConnectionsPerNodeInMemory, c.Source, c.Level, maxConnectionsPerNodeInMemory)
		targets = targets[:maxConnectionsPerNodeInMemory]
	}

	newNodes, changed, err := growIndexToAccommodateNode(res.Nodes, c.Source, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[c.Source] == nil {
		res.Nodes[c.Source] = &Vertex{ID: c.Source}
	}

	if res.Nodes[c.Source].Connections == nil {
		res.Nodes[c.Source].Connections = &packedconn.Connections{}
	} else {
		res.Nodes[c.Source].Connections.GrowLayersTo(uint8(c.Level))
	}

	res.Nodes[c.Source].Connections.ReplaceLayer(uint8(c.Level), targets)

	if keepReplaceInfo {
		// Mark the replace flag for this node and level, so that new commit logs
		// generated on this result (condensing) do not lose information.
		// This is critical for correctly handling .condensed files.
		if _, ok := res.LinksReplaced[c.Source]; !ok {
			res.LinksReplaced[c.Source] = map[uint16]struct{}{}
		}
		res.LinksReplaced[c.Source][c.Level] = struct{}{}
	}

	return nil
}

func (r *InMemoryReader) readClearLinks(c *ClearLinksCommit, res *DeserializationResult, keepReplaceInfo bool) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Nodes, c.ID, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if res.Nodes[c.ID] == nil {
		// node has been deleted or never existed, nothing to do
		return nil
	}

	res.Nodes[c.ID].Connections, err = packedconn.NewWithMaxLayer(uint8(res.Nodes[c.ID].Level))
	return err
}

func (r *InMemoryReader) readClearLinksAtLevel(c *ClearLinksAtLevelCommit, res *DeserializationResult, keepReplaceInfo bool) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Nodes, c.ID, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	if keepReplaceInfo {
		// Mark the replace flag for this node and level, so that new commit logs
		// generated on this result (condensing) do not lose information.
		if _, ok := res.LinksReplaced[c.ID]; !ok {
			res.LinksReplaced[c.ID] = map[uint16]struct{}{}
		}
		res.LinksReplaced[c.ID][c.Level] = struct{}{}
	}

	if res.Nodes[c.ID] == nil {
		if !keepReplaceInfo {
			// node has been deleted or never existed and we are not looking at a
			// single log in isolation, nothing to do
			return nil
		}

		// we need to keep the replace info, meaning we have to explicitly create
		// this node in order to be able to store the "clear links" information for it
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Nodes[c.ID] = &Vertex{
			ID:          c.ID,
			Connections: conns,
		}
	}

	if res.Nodes[c.ID].Connections == nil {
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Nodes[c.ID].Connections = conns
	} else {
		res.Nodes[c.ID].Connections.GrowLayersTo(uint8(c.Level))
		// Only clear if the layer is not already empty
		if res.Nodes[c.ID].Connections.LenAtLayer(uint8(c.Level)) > 0 {
			res.Nodes[c.ID].Connections.ClearLayer(uint8(c.Level))
		}
	}

	if keepReplaceInfo {
		// Mark the replace flag for this node and level again
		// (duplicated for consistency with original deserializer)
		if _, ok := res.LinksReplaced[c.ID]; !ok {
			res.LinksReplaced[c.ID] = map[uint16]struct{}{}
		}
		res.LinksReplaced[c.ID][c.Level] = struct{}{}
	}

	return nil
}

func (r *InMemoryReader) readDeleteNode(c *DeleteNodeCommit, res *DeserializationResult) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Nodes, c.ID, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Nodes = newNodes
	}

	res.Nodes[c.ID] = nil
	res.NodesDeleted[c.ID] = struct{}{}
	return nil
}

// growIndexToAccommodateNode grows the nodes slice if needed to accommodate the given ID.
// Returns the new slice (if grown), whether it changed, and any error.
func growIndexToAccommodateNode(index []*Vertex, id uint64, logger logrus.FieldLogger) ([]*Vertex, bool, error) {
	previousSize := uint64(len(index))
	if id < previousSize {
		// node will fit, nothing to do
		return nil, false, nil
	}

	var newSize uint64

	if (indexGrowthRate-1)*float64(previousSize) < float64(cache.MinimumIndexGrowthDelta) {
		// typically grow the index by the delta
		newSize = previousSize + cache.MinimumIndexGrowthDelta
	} else {
		newSize = uint64(float64(previousSize) * indexGrowthRate)
	}

	if newSize <= id {
		// There are situations where docIDs are not in order. For example, if the
		// default size is 10k and the default delta is 10k. Imagine the user
		// imports 21 objects, then deletes the first 20,500. When rebuilding the
		// index from disk the first id to be imported would be 20,501, however the
		// index default size and default delta would only reach up to 20,000.
		newSize = id + cache.MinimumIndexGrowthDelta
	}

	newIndex := make([]*Vertex, newSize)
	copy(newIndex, index)

	logger.WithField("action", "hnsw_grow_index").
		WithField("previous_size", previousSize).
		WithField("new_size", newSize).
		Debugf("index grown from %d to %d", previousSize, newSize)

	return newIndex, true, nil
}
