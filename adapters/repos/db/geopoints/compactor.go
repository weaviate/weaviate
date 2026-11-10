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

package geopoints

import (
	"context"
	"fmt"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/compactor"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Compactor merges two segments into one tree. right is the newer segment.
// The surviving points are held in memory while the tree is rebuilt, once:
// MergeMemoryEstimate tells the caller how much that is, so it can be checked
// against the memory available before the segments are read.
type Compactor struct {
	left, right  *SegmentReader
	currentLevel uint16
	// cleanupDeletions is set when left is the oldest segment: no older segment
	// can hold a point the deletions refer to, so none is kept.
	cleanupDeletions         bool
	enableChecksumValidation bool

	w    io.WriteSeeker
	bufw compactor.Writer
	mw   *compactor.MemoryWriter
}

func NewCompactor(w io.WriteSeeker, left, right *SegmentReader,
	level uint16, cleanupDeletions bool, enableChecksumValidation bool, maxNewFileSize int64,
) *Compactor {
	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"operation": "compaction",
		"strategy":  segmentindex.StrategyGeoPoints.String(),
	})
	meteredW := diskio.NewMeteredWriter(w, func(written int64) {
		observeWrite.Observe(float64(written))
	})
	writer, mw := compactor.NewWriter(meteredW, maxNewFileSize)

	return &Compactor{
		left:                     left,
		right:                    right,
		w:                        meteredW,
		bufw:                     writer,
		mw:                       mw,
		currentLevel:             level,
		cleanupDeletions:         cleanupDeletions,
		enableChecksumValidation: enableChecksumValidation,
	}
}

func (c *Compactor) Do(ctx context.Context) error {
	// a placeholder header: the real one is written once the body size is known
	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return fmt.Errorf("write empty header: %w", err)
	}

	body, err := Merge(ctx, c.left, c.right, c.cleanupDeletions)
	if err != nil {
		return fmt.Errorf("merge segments: %w", err)
	}

	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(c.bufw),
		segmentindex.WithChecksumsDisabled(!c.enableChecksumValidation),
	)
	if _, err := body.WriteTo(segmentFile.BodyWriter()); err != nil {
		return fmt.Errorf("write body: %w", err)
	}

	// flush buffered, so we can safely seek on underlying writer
	if c.mw == nil {
		if err := c.bufw.Flush(); err != nil {
			return fmt.Errorf("flush buffered: %w", err)
		}
	}

	dataEnd := segmentindex.HeaderSize + uint64(body.Size())
	version := segmentindex.ChooseHeaderVersion(c.enableChecksumValidation)
	if err := compactor.WriteHeader(c.mw, c.w, c.bufw, segmentFile, c.currentLevel, version,
		0, dataEnd, segmentindex.StrategyGeoPoints); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write geopoints segment checksum: %w", err)
	}
	return nil
}

// MergeMemoryEstimate is the heap Merge needs for two segments: every point
// of both, the nodes of the tree over them, the deletion bitmaps and the
// scratch of a chunked column read. The points dominate at 16 bytes each.
func MergeMemoryEstimate(left, right *SegmentReader) int64 {
	points := left.hdr.pointCount + right.hdr.pointCount
	// a median split makes at most one leaf per 256 points, two nodes per leaf
	nodes := points/128 + 1
	deletions := 2 * (left.hdr.deletionsLen + right.hdr.deletionsLen)
	return int64(points*16 + nodes*nodeSize + deletions + 3*pointChunk*8)
}

// Merge computes the body of the segment replacing left and right. A point
// deleted by either segment is dropped and its deletion spent: a doc ID is
// added once in its lifetime, so the deletion can refer to no other point.
// The deletion may sit in the older segment and the point in the newer one
// when a write lost a race with the delete of its object.
func Merge(ctx context.Context, left, right *SegmentReader, cleanupDeletions bool) (*Body, error) {
	leftDel, err := left.Deletions()
	if err != nil {
		return nil, fmt.Errorf("left deletions: %w", err)
	}
	rightDel, err := right.Deletions()
	if err != nil {
		return nil, fmt.Errorf("right deletions: %w", err)
	}
	deleted := leftDel.Clone()
	deleted.Or(rightDel)

	// the deletions carried on for older segments, less those spent here
	var carried *sroar.Bitmap
	if !cleanupDeletions {
		carried = deleted.Clone()
	}
	var drop func(docID uint64) bool
	if !deleted.IsEmpty() {
		drop = func(docID uint64) bool {
			if !deleted.Contains(docID) {
				return false
			}
			if carried != nil {
				carried.Remove(docID)
			}
			return true
		}
	}

	points := make([]Point, 0, left.PointCount()+right.PointCount())
	points, err = left.appendPoints(points, drop)
	if err != nil {
		return nil, fmt.Errorf("left points: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	points, err = right.appendPoints(points, drop)
	if err != nil {
		return nil, fmt.Errorf("right points: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	body := &Body{Points: points}
	if carried != nil && !carried.IsEmpty() {
		body.Deletions = carried.ToBuffer()
	}
	return body, nil
}
