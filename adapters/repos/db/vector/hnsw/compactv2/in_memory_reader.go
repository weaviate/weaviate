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

package compactv2

import (
	"io"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

const (
	maxConnectionsPerNodeInMemory = 4096
	indexGrowthRate               = 1.2
)

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
func (r *InMemoryReader) Do(initialState *ent.DeserializationResult, keepLinkReplaceInformation bool) (*ent.DeserializationResult, error) {
	out := initialState
	commitTypeMetrics := make(map[HnswCommitType]int)

	if out == nil {
		out = ent.NewDeserializationResult(cache.InitialSize)
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
			out.Graph.Entrypoint = commit.Entrypoint
			out.Graph.Level = commit.Level
			out.Graph.EntrypointChanged = true
		case *AddLinkAtLevelCommit:
			err = r.readLink(commit, out)
		case *AddLinksAtLevelCommit:
			err = r.readAddLinks(commit, out)
		case *ReplaceLinksAtLevelCommit:
			err = r.readReplaceLinks(commit, out, keepLinkReplaceInformation)
		case *AddTombstoneCommit:
			out.Graph.Tombstones[commit.ID] = struct{}{}
		case *RemoveTombstoneCommit:
			_, ok := out.Graph.Tombstones[commit.ID]
			if !ok {
				// Tombstone is not present but may exist in older commit log
				// We need to keep track of it so we can delete it later
				out.Graph.TombstonesDeleted[commit.ID] = struct{}{}
			} else {
				// Tombstone is present, we can delete it
				delete(out.Graph.Tombstones, commit.ID)
			}
		case *ClearLinksCommit:
			err = r.readClearLinks(commit, out, keepLinkReplaceInformation)
		case *ClearLinksAtLevelCommit:
			err = r.readClearLinksAtLevel(commit, out, keepLinkReplaceInformation)
		case *DeleteNodeCommit:
			err = r.readDeleteNode(commit, out)
		case *ResetIndexCommit:
			out.Graph.Entrypoint = 0
			out.Graph.Level = 0
			out.Graph.Nodes = make([]*ent.Vertex, cache.InitialSize)
			out.Graph.Tombstones = make(map[uint64]struct{})
			// Reset compression state - ResetIndex clears everything
			out.Compression = nil
		case *AddPQCommit:
			out.SetCompressionPQData(commit.Data)
			out.SetCompressed(true)
		case *AddSQCommit:
			out.SetCompressionSQData(commit.Data)
			out.SetCompressed(true)
		case *AddRQCommit:
			out.SetCompressionRQData(commit.Data)
			out.SetCompressed(true)
		case *AddBRQCommit:
			out.SetCompressionBRQData(commit.Data)
			out.SetCompressed(true)
		case *AddMuveraCommit:
			out.SetEncoderMuvera(commit.Data)
			out.SetMuveraEnabled(true)
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

func (r *InMemoryReader) readNode(c *AddNodeCommit, res *ent.DeserializationResult) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Graph.Nodes, c.ID, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Graph.Nodes = newNodes
	}

	if res.Graph.Nodes[c.ID] == nil {
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Graph.Nodes[c.ID] = &ent.Vertex{
			Level:       int(c.Level),
			ID:          c.ID,
			Connections: conns,
		}
	} else {
		if res.Graph.Nodes[c.ID].Connections == nil {
			res.Graph.Nodes[c.ID].Connections, err = packedconn.NewWithMaxLayer(uint8(c.Level))
			if err != nil {
				return err
			}
		} else {
			res.Graph.Nodes[c.ID].Connections.GrowLayersTo(uint8(c.Level))
		}
		res.Graph.Nodes[c.ID].Level = int(c.Level)
	}
	return nil
}

func (r *InMemoryReader) readLink(c *AddLinkAtLevelCommit, res *ent.DeserializationResult) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Graph.Nodes, c.Source, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Graph.Nodes = newNodes
	}

	if res.Graph.Nodes[c.Source] == nil {
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Graph.Nodes[c.Source] = &ent.Vertex{
			ID:          c.Source,
			Connections: conns,
		}
	}

	if res.Graph.Nodes[c.Source].Connections == nil {
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Graph.Nodes[c.Source].Connections = conns
	} else {
		res.Graph.Nodes[c.Source].Connections.GrowLayersTo(uint8(c.Level))
	}

	res.Graph.Nodes[c.Source].Connections.InsertAtLayer(c.Target, uint8(c.Level))
	return nil
}

func (r *InMemoryReader) readAddLinks(c *AddLinksAtLevelCommit, res *ent.DeserializationResult) error {
	targets := c.Targets
	if len(targets) >= maxConnectionsPerNodeInMemory {
		r.logger.Warnf("read AddLinksAtLevel with %v (>= %d) connections for node %d at level %d, truncating to %d",
			len(targets), maxConnectionsPerNodeInMemory, c.Source, c.Level, maxConnectionsPerNodeInMemory)
		targets = targets[:maxConnectionsPerNodeInMemory]
	}

	newNodes, changed, err := growIndexToAccommodateNode(res.Graph.Nodes, c.Source, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Graph.Nodes = newNodes
	}

	if res.Graph.Nodes[c.Source] == nil {
		res.Graph.Nodes[c.Source] = &ent.Vertex{ID: c.Source}
	}

	if res.Graph.Nodes[c.Source].Connections == nil {
		res.Graph.Nodes[c.Source].Connections = &packedconn.Connections{}
	} else {
		res.Graph.Nodes[c.Source].Connections.GrowLayersTo(uint8(c.Level))
	}

	res.Graph.Nodes[c.Source].Connections.BulkInsertAtLayer(targets, uint8(c.Level))
	return nil
}

func (r *InMemoryReader) readReplaceLinks(c *ReplaceLinksAtLevelCommit, res *ent.DeserializationResult, keepReplaceInfo bool) error {
	targets := c.Targets
	if len(targets) >= maxConnectionsPerNodeInMemory {
		r.logger.Warnf("read ReplaceLinksAtLevel with %v (>= %d) connections for node %d at level %d, truncating to %d",
			len(targets), maxConnectionsPerNodeInMemory, c.Source, c.Level, maxConnectionsPerNodeInMemory)
		targets = targets[:maxConnectionsPerNodeInMemory]
	}

	newNodes, changed, err := growIndexToAccommodateNode(res.Graph.Nodes, c.Source, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Graph.Nodes = newNodes
	}

	if res.Graph.Nodes[c.Source] == nil {
		res.Graph.Nodes[c.Source] = &ent.Vertex{ID: c.Source}
	}

	if res.Graph.Nodes[c.Source].Connections == nil {
		res.Graph.Nodes[c.Source].Connections = &packedconn.Connections{}
	} else {
		res.Graph.Nodes[c.Source].Connections.GrowLayersTo(uint8(c.Level))
	}

	res.Graph.Nodes[c.Source].Connections.ReplaceLayer(uint8(c.Level), targets)

	if keepReplaceInfo {
		// Mark the replace flag for this node and level, so that new commit logs
		// generated on this result (condensing) do not lose information.
		// This is critical for correctly handling .condensed files.
		if _, ok := res.Graph.LinksReplaced[c.Source]; !ok {
			res.Graph.LinksReplaced[c.Source] = map[uint16]struct{}{}
		}
		res.Graph.LinksReplaced[c.Source][c.Level] = struct{}{}
	}

	return nil
}

func (r *InMemoryReader) readClearLinks(c *ClearLinksCommit, res *ent.DeserializationResult, keepReplaceInfo bool) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Graph.Nodes, c.ID, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Graph.Nodes = newNodes
	}

	if res.Graph.Nodes[c.ID] == nil {
		// node has been deleted or never existed, nothing to do
		return nil
	}

	res.Graph.Nodes[c.ID].Connections, err = packedconn.NewWithMaxLayer(uint8(res.Graph.Nodes[c.ID].Level))
	return err
}

func (r *InMemoryReader) readClearLinksAtLevel(c *ClearLinksAtLevelCommit, res *ent.DeserializationResult, keepReplaceInfo bool) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Graph.Nodes, c.ID, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Graph.Nodes = newNodes
	}

	if keepReplaceInfo {
		// Mark the replace flag for this node and level, so that new commit logs
		// generated on this result (condensing) do not lose information.
		if _, ok := res.Graph.LinksReplaced[c.ID]; !ok {
			res.Graph.LinksReplaced[c.ID] = map[uint16]struct{}{}
		}
		res.Graph.LinksReplaced[c.ID][c.Level] = struct{}{}
	}

	if res.Graph.Nodes[c.ID] == nil {
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
		res.Graph.Nodes[c.ID] = &ent.Vertex{
			ID:          c.ID,
			Connections: conns,
		}
	}

	if res.Graph.Nodes[c.ID].Connections == nil {
		conns, err := packedconn.NewWithMaxLayer(uint8(c.Level))
		if err != nil {
			return err
		}
		res.Graph.Nodes[c.ID].Connections = conns
	} else {
		res.Graph.Nodes[c.ID].Connections.GrowLayersTo(uint8(c.Level))
		// Only clear if the layer is not already empty
		if res.Graph.Nodes[c.ID].Connections.LenAtLayer(uint8(c.Level)) > 0 {
			res.Graph.Nodes[c.ID].Connections.ClearLayer(uint8(c.Level))
		}
	}

	if keepReplaceInfo {
		// Mark the replace flag for this node and level again
		// (duplicated for consistency with original deserializer)
		if _, ok := res.Graph.LinksReplaced[c.ID]; !ok {
			res.Graph.LinksReplaced[c.ID] = map[uint16]struct{}{}
		}
		res.Graph.LinksReplaced[c.ID][c.Level] = struct{}{}
	}

	return nil
}

func (r *InMemoryReader) readDeleteNode(c *DeleteNodeCommit, res *ent.DeserializationResult) error {
	newNodes, changed, err := growIndexToAccommodateNode(res.Graph.Nodes, c.ID, r.logger)
	if err != nil {
		return err
	}

	if changed {
		res.Graph.Nodes = newNodes
	}

	res.Graph.Nodes[c.ID] = nil
	res.Graph.NodesDeleted[c.ID] = struct{}{}
	return nil
}

// growIndexToAccommodateNode grows the nodes slice if needed to accommodate the given ID.
// Returns the new slice (if grown), whether it changed, and any error.
func growIndexToAccommodateNode(index []*ent.Vertex, id uint64, logger logrus.FieldLogger) ([]*ent.Vertex, bool, error) {
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

	newIndex := make([]*ent.Vertex, newSize)
	copy(newIndex, index)

	logger.WithField("action", "hnsw_grow_index").
		WithField("previous_size", previousSize).
		WithField("new_size", newSize).
		Debugf("index grown from %d to %d", previousSize, newSize)

	return newIndex, true, nil
}
