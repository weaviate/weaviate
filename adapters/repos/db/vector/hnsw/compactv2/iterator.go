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
	"errors"
	"io"

	"github.com/sirupsen/logrus"
)

// NodeCommits represents all commits for a single node.
type NodeCommits struct {
	NodeID  uint64
	Commits []Commit
}

// CommitReader is an interface for reading commits from a WAL.
// This allows for easier testing and mocking.
type CommitReader interface {
	ReadNextCommit() (Commit, error)
}

// Iterator wraps a CommitReader and provides node-level iteration.
// Unlike CommitReader which is commit-specific, Iterator is node-specific
// and returns all commits for a node at once.
type Iterator struct {
	reader          CommitReader
	logger          logrus.FieldLogger
	id              int // ordinal ID for precedence in merging
	globalCommits   []Commit
	currentNode     *NodeCommits
	exhausted       bool
	peekedCommit    Commit
	hasPeekedCommit bool
}

// NewIterator creates a new Iterator from a CommitReader.
// The id parameter is used for merge precedence (higher ID = more recent log).
func NewIterator(reader CommitReader, id int, logger logrus.FieldLogger) (*Iterator, error) {
	it := &Iterator{
		reader:        reader,
		logger:        logger,
		id:            id,
		globalCommits: make([]Commit, 0),
	}

	// Read all global commits first
	if err := it.readGlobalCommits(); err != nil {
		return nil, err
	}

	// Advance to the first node
	if err := it.advance(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return it, nil
}

// ID returns the iterator's ordinal ID (used for merge precedence).
func (it *Iterator) ID() int {
	return it.id
}

// GlobalCommits returns all global (non-node-specific) commits from the beginning of the file.
func (it *Iterator) GlobalCommits() []Commit {
	return it.globalCommits
}

// Current returns the commits for the current node.
// Returns nil if the iterator is exhausted.
func (it *Iterator) Current() *NodeCommits {
	return it.currentNode
}

// Exhausted returns true if there are no more nodes to read.
func (it *Iterator) Exhausted() bool {
	return it.exhausted
}

// Next advances the iterator to the next node.
// Returns true if there is a next node, false if exhausted.
func (it *Iterator) Next() (bool, error) {
	if it.exhausted {
		return false, nil
	}

	if err := it.advance(); err != nil {
		if errors.Is(err, io.EOF) {
			it.exhausted = true
			it.currentNode = nil
			return false, nil
		}
		return false, err
	}

	return !it.exhausted, nil
}

// readGlobalCommits reads all global commits from the beginning of the file.
// Global commits are those without a node ID (compression, muvera, entrypoint).
func (it *Iterator) readGlobalCommits() error {
	for {
		c, err := it.reader.ReadNextCommit()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// File is all global commits
				return nil
			}
			return err
		}

		if isGlobalCommit(c) {
			it.globalCommits = append(it.globalCommits, c)
		} else {
			// First node-specific commit - save it for later
			it.peekedCommit = c
			it.hasPeekedCommit = true
			return nil
		}
	}
}

// advance reads all commits for the next node and sets it as current.
func (it *Iterator) advance() error {
	var commits []Commit
	var currentNodeID uint64
	nodeIDSet := false

	// If we have a peeked commit, start with that
	if it.hasPeekedCommit {
		nodeID, hasNodeID := extractNodeID(it.peekedCommit)
		if hasNodeID {
			currentNodeID = nodeID
			nodeIDSet = true
			commits = append(commits, it.peekedCommit)
		}
		it.hasPeekedCommit = false
		it.peekedCommit = nil
	}

	// Read commits until we hit a different node ID or EOF
	for {
		c, err := it.reader.ReadNextCommit()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if nodeIDSet {
					// We have commits for the current node, return them
					it.currentNode = &NodeCommits{
						NodeID:  currentNodeID,
						Commits: commits,
					}
					return nil
				}
				// No more data
				it.exhausted = true
				it.currentNode = nil
				return io.EOF
			}
			return err
		}

		nodeID, hasNodeID := extractNodeID(c)
		if !hasNodeID {
			// This shouldn't happen in a sorted file - global commits should be at the start
			it.logger.Warnf("Found global commit in middle of sorted file: %T", c)
			continue
		}

		if !nodeIDSet {
			// First commit for a node
			currentNodeID = nodeID
			nodeIDSet = true
			commits = append(commits, c)
		} else if nodeID == currentNodeID {
			// Same node, add commit
			commits = append(commits, c)
		} else {
			// Different node - save for next iteration
			it.peekedCommit = c
			it.hasPeekedCommit = true
			it.currentNode = &NodeCommits{
				NodeID:  currentNodeID,
				Commits: commits,
			}
			return nil
		}
	}
}

// isGlobalCommit returns true if the commit is global (has no node ID).
func isGlobalCommit(c Commit) bool {
	switch c.(type) {
	case *SetEntryPointMaxLevelCommit, *ResetIndexCommit,
		*AddPQCommit, *AddSQCommit, *AddRQCommit, *AddBRQCommit, *AddMuveraCommit:
		return true
	default:
		return false
	}
}

// extractNodeID extracts the node ID from a commit if it has one.
// Returns (nodeID, true) if the commit has a node ID, (0, false) otherwise.
func extractNodeID(c Commit) (uint64, bool) {
	switch ct := c.(type) {
	case *AddNodeCommit:
		return ct.ID, true
	case *DeleteNodeCommit:
		return ct.ID, true
	case *AddLinkAtLevelCommit:
		return ct.Source, true
	case *AddLinksAtLevelCommit:
		return ct.Source, true
	case *ReplaceLinksAtLevelCommit:
		return ct.Source, true
	case *ClearLinksCommit:
		return ct.ID, true
	case *ClearLinksAtLevelCommit:
		return ct.ID, true
	case *AddTombstoneCommit:
		return ct.ID, true
	case *RemoveTombstoneCommit:
		return ct.ID, true
	default:
		return 0, false
	}
}
