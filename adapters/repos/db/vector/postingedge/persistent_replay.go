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

package postingedge

import (
	"slices"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// A replay is a batch the queue of a shard hands over again: it deletes a
// chunk only once the index has flushed what it was given, so a process that
// dies before that, or a flush that fails, has the index given vectors it
// holds, under their doc ids, maybe next to some it does not hold. Refused,
// the batch would be a permanent error to the worker of the queue, which
// discards it: the vectors of it the index does not hold would never be
// indexed. So a doc id the index holds is left out of the batch: given again
// with the vector the index holds, it is a replay; with another vector, a
// conflict, which the index logs, keeping the vector it holds. HNSW and
// HFresh insert such a vector again, and log nothing.

// leaveOutPlaced leaves a doc id the index holds out of the batch that gives
// it again, with its vector as the index stores it: a replay, or a conflict
// it logs. A vector it cannot read to compare is left out all the same, and
// logged: failing the batch would lose the rest of it. The caller holds
// writeMu.
func (p *Persistent) leaveOutPlaced(id uint64, normalized []float32) {
	if _, gone := p.gone[id]; gone {
		// Deleted while the base is built, which may hold it: its doc id is
		// not given again until the base is the index, and a replay of its
		// insert comes before the replay of its delete.
		return
	}
	if g := p.g; g != nil && g.deleted != nil && id < uint64(len(g.deleted)) && g.deleted[id] {
		// Marked deleted, and in its posting until the pass over the deleted
		// vectors takes it out: the replay of its insert comes before the
		// replay of its delete. So is a vector the index left out because
		// its file did not hold it (growIndex.loseVectors).
		return
	}
	stored, err := p.storedVector(id)
	if err != nil {
		p.logger.WithFields(logrus.Fields{"action": "postingedge_vector_conflict", "id": id}).
			Warnf("doc id %d was given again, and the vector the index holds could not be read to compare: "+
				"the index keeps it, and leaves the one given out: %v", id, err)
		return
	}
	if !slices.Equal(stored, normalized) {
		p.logConflict(id)
	}
}

// leaveOutReplayed returns, of a batch, the doc ids and the vectors the index
// takes: those of the doc ids it does not hold, each once. The others are a
// replay, or a conflict it logs (leaveOutPlaced); a doc id given twice in the
// batch goes in once, with the first of its vectors. The caller holds
// writeMu.
func (p *Persistent) leaveOutReplayed(ids []uint64, vectors [][]float32) ([]uint64, [][]float32) {
	first := make(map[uint64]int, len(ids))
	out := 0
	for i, id := range ids {
		if _, repeated := first[id]; repeated {
			out++
			continue
		}
		first[id] = i
		if p.placed(id) {
			out++
		}
	}
	if out == 0 {
		return ids, vectors
	}
	keptIDs, kept := make([]uint64, 0, len(ids)-out), make([][]float32, 0, len(ids)-out)
	for i, id := range ids {
		if at := first[id]; at != i {
			if !slices.Equal(distancer.Normalize(vectors[at]), distancer.Normalize(vectors[i])) {
				p.logConflict(id)
			}
			continue
		}
		if p.placed(id) {
			p.leaveOutPlaced(id, distancer.Normalize(vectors[i]))
			continue
		}
		keptIDs, kept = append(keptIDs, id), append(kept, vectors[i])
	}
	return keptIDs, kept
}

// logConflict logs a doc id given again with another vector than the one the
// index keeps.
func (p *Persistent) logConflict(id uint64) {
	p.logger.WithFields(logrus.Fields{"action": "postingedge_vector_conflict", "id": id}).
		Warnf("doc id %d was given again with another vector: the index keeps the vector it holds, "+
			"and leaves the other out", id)
}

// storedVector returns the vector the index holds under a doc id it placed,
// as it stores it: kept aside, or in a posting, where a deleted vector still
// is until the cleanup takes it out. The caller holds writeMu.
func (p *Persistent) storedVector(id uint64) ([]float32, error) {
	if _, staged := p.staged[id]; staged {
		raw, err := p.staging.Get(growDocKey(id))
		if err != nil {
			return nil, errors.Wrapf(err, "read the vector kept aside of doc %d", id)
		}
		if len(raw)%4 != 0 || isGoneMark(raw) {
			return nil, errors.Errorf("the vector kept aside of doc %d takes %d bytes", id, len(raw))
		}
		vector := make([]float32, len(raw)/4)
		for i := range vector {
			vector[i] = getFloat32(raw[4*i:])
		}
		return vector, nil
	}
	if g := p.g; g != nil && id < uint64(len(g.live)) && g.live[id] {
		_, vector, err := g.exact(id, nil, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "read the vector of doc %d", id)
		}
		return vector, nil
	}
	return nil, errors.Errorf("doc %d is not placed", id)
}
