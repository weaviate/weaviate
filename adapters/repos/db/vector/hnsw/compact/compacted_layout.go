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

import "github.com/pkg/errors"

// compactedLayout checks each record of a .sorted or .condensed segment against
// the order its writers produce, so garbage appended to the segment is caught
// before it is applied (weaviate/0-weaviate-issues#666):
//
//   - .sorted (SortedWriter, n-way merge): compression records (PQ, SQ, RQ, BRQ
//     order), muvera, entrypoint, then node records in ascending node ID.
//   - .condensed (legacy MemoryCondensor): compression, muvera, node records;
//     the entrypoint follows the node records.
//
// Neither ever contains a ResetIndex: it is consumed when a raw file is
// converted. Garbage that decodes as a correctly placed record is not caught
// here, including compression records at the head of an empty segment; that
// needs a checksum. A zeroed block decodes as AddNode(0) and is caught in .sorted.
type compactedLayout struct {
	fileType        FileType
	nodesStarted    bool
	muveraSeen      bool
	entrypointSeen  bool
	lastCompression int
	lastNodeID      uint64
}

func (l *compactedLayout) check(c Commit) error {
	switch c.(type) {
	case *ResetIndexCommit:
		return l.reject(c)
	case *AddPQCommit, *AddSQCommit, *AddRQCommit, *AddBRQCommit:
		rank := compressionRank(c)
		if l.nodesStarted || l.muveraSeen || l.entrypointSeen || rank <= l.lastCompression {
			return l.reject(c)
		}
		l.lastCompression = rank
	case *AddMuveraCommit:
		if l.nodesStarted || l.muveraSeen || l.entrypointSeen {
			return l.reject(c)
		}
		l.muveraSeen = true
	case *SetEntryPointMaxLevelCommit:
		if l.entrypointSeen || (l.fileType == FileTypeSorted && l.nodesStarted) {
			return l.reject(c)
		}
		l.entrypointSeen = true
	default:
		id, ok := extractNodeID(c)
		if ok && l.fileType == FileTypeSorted {
			if l.nodesStarted && id < l.lastNodeID {
				return errors.Errorf("%s record for node %d after node %d in sorted segment", c.Type(), id, l.lastNodeID)
			}
			l.lastNodeID = id
		}
		l.nodesStarted = true
	}
	return nil
}

func (l *compactedLayout) reject(c Commit) error {
	return errors.Errorf("%s record out of place in %s segment", c.Type(), l.fileType)
}

// compressionRank orders compression records the way the writers emit them.
func compressionRank(c Commit) int {
	switch c.(type) {
	case *AddPQCommit:
		return 1
	case *AddSQCommit:
		return 2
	case *AddRQCommit:
		return 3
	case *AddBRQCommit:
		return 4
	default:
		return 0
	}
}
