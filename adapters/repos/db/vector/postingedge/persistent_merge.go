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
	"time"

	"github.com/sirupsen/logrus"
)

// The bucket of postings as one segment (bench-report.md, "v5.3"). A load of
// a posting reads its two rows, and with the bucket in one segment that is
// two reads of the store and what the rows weigh. Between compactions a row
// of edges that was written again is in several segments, with the
// tombstones that carry its old versions, and a load reads all of them.
//
// The store does not leave the bucket as one segment by itself: it compacts
// two segments of the same level, and two left-over ones only if they are of
// similar sizes, so the large segment stays next to the small ones that came
// after it. That was measured at 1M (integration-plan.md, section 7): three
// segments, and 3.7 reads a load where one segment gives 2.0.
//
// So the index asks: once enough has been written since the last time, it
// has the store compact the left-over segments of the bucket whatever their
// sizes (lsmkv.Bucket.CompactLeftOverSegmentsOfAnySize), which the cycles of
// the store then do, and takes the request back when the bucket is one
// segment. The cycles are those of the node: the index only looks, every
// mergeCheckEvery, at whether they are done.

// mergeCheckEvery is how often what runs in the background looks at whether
// the bucket of postings is due to be left as one segment, or has been.
const mergeCheckEvery = 2 * time.Second

// mergeShare is the share of the vectors of the index that must have been
// written since the bucket of postings was last one segment for it to be
// asked for again. The rule is provisional (integration-plan.md, section 7).
const mergeShare = 0.1

// mergeDue tells whether the bucket of postings has more than one segment
// and enough was written since it was last asked to be one.
func (g *growIndex) mergeDue() bool {
	return g.shared && !g.merging && float64(g.sinceMerge) >= mergeShare*float64(g.held()) && g.sinceMerge > 0 &&
		g.postings.SegmentCount() > 1
}

// beginMerge asks the store to leave the bucket of postings as one segment.
func (g *growIndex) beginMerge() {
	g.postings.CompactLeftOverSegmentsOfAnySize(true)
	g.merging, g.sinceMerge = true, 0
}

// endMerge takes the request back, if the store is done with it, and tells
// whether it was.
func (g *growIndex) endMerge() bool {
	if !g.merging || g.postings.SegmentCount() > 1 {
		return false
	}
	g.postings.CompactLeftOverSegmentsOfAnySize(false)
	g.merging = false
	g.merges++
	return true
}

// mergeRows asks for the bucket of postings as one segment if that is due,
// and takes the request back once the store is done.
func (p *Persistent) mergeRows() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.failed != nil || p.g == nil {
		return
	}
	g := p.g
	switch {
	case g.endMerge():
		p.logger.WithFields(logrus.Fields{
			"action": "postingedge_merge",
			"id":     p.cfg.ID,
		}).Info("the bucket of postings is one segment")
	case g.mergeDue():
		g.beginMerge()
	}
}
