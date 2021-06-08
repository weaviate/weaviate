package lsmkv

import (
	"math"
	"os"

	"github.com/pkg/errors"
)

func (ig *SegmentGroup) eligbleForCompaction() bool {
	// if there are at least two segments of the same level a regular compaction
	// can be performed

	levels := map[uint16]int{}

	for _, segment := range ig.segments {
		levels[segment.level]++
		if levels[segment.level] > 1 {
			return true
		}
	}

	return false
}

func (ig *SegmentGroup) bestCompactionCandidatePair() []int {
	ig.maintenanceLock.RLock()
	defer ig.maintenanceLock.RUnlock()

	// first determine the lowest level with candidates
	levels := map[uint16]int{}

	for _, segment := range ig.segments {
		levels[segment.level]++
	}

	currLowestLevel := uint16(math.MaxUint16)
	found := false
	for level, count := range levels {
		if count < 2 {
			continue
		}

		if level < currLowestLevel {
			currLowestLevel = level
			found = true
		}
	}

	if !found {
		return nil
	}

	// now pick any two segements which match the level
	var res []int

	for i, segment := range ig.segments {
		if len(res) >= 2 {
			break
		}

		if segment.level == currLowestLevel {
			res = append(res, i)
		}
	}

	return res
}

func (ig *SegmentGroup) compactOnce() error {
	pair := ig.bestCompactionCandidatePair()
	if pair == nil {
		// nothing to do
		return nil
	}

	if ig.segments[pair[0]].strategy != SegmentStrategyReplace {
		return errors.Errorf("only replace supported at the moment")
	}

	f, err := os.Create("todobar")
	if err != nil {
		return err
	}

	c := newCompactorReplace(f, ig.segments[pair[0]].newCursor(),
		ig.segments[pair[1]].newCursor())

	if err := c.do(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return errors.Wrap(err, "close compacted segment file")
	}

	return nil
}
