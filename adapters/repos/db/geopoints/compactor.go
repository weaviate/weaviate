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

// Compactor merges two segments into one tree. right is the newer segment:
// its deletions remove points of left, and a deletion that found its point in
// either segment is spent. Both point sets are held in memory while the tree
// is rebuilt, which the bucket's maximum segment size bounds.
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
	if _, err := segmentFile.BodyWriter().Write(body); err != nil {
		return fmt.Errorf("write body: %w", err)
	}

	// flush buffered, so we can safely seek on underlying writer
	if c.mw == nil {
		if err := c.bufw.Flush(); err != nil {
			return fmt.Errorf("flush buffered: %w", err)
		}
	}

	dataEnd := segmentindex.HeaderSize + uint64(len(body))
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

// Merge computes the body of the segment replacing left and right.
func Merge(ctx context.Context, left, right *SegmentReader, cleanupDeletions bool) ([]byte, error) {
	leftDel, err := left.Deletions()
	if err != nil {
		return nil, fmt.Errorf("left deletions: %w", err)
	}
	rightDel, err := right.Deletions()
	if err != nil {
		return nil, fmt.Errorf("right deletions: %w", err)
	}
	leftPoints, err := left.Points()
	if err != nil {
		return nil, fmt.Errorf("left points: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rightPoints, err := right.Points()
	if err != nil {
		return nil, fmt.Errorf("right points: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// the union is taken before points are dropped, so a spent deletion can be
	// removed from it below
	var merged *sroar.Bitmap
	if !cleanupDeletions {
		merged = leftDel.Clone()
		merged.Or(rightDel)
	}

	points := make([]Point, 0, len(leftPoints)+len(rightPoints))
	keep := func(p Point) {
		if rightDel.Contains(p.DocID) {
			if merged != nil {
				merged.Remove(p.DocID)
			}
			return
		}
		points = append(points, p)
	}
	for _, p := range leftPoints {
		keep(p)
	}
	for _, p := range rightPoints {
		keep(p)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var deletions []byte
	if merged != nil && !merged.IsEmpty() {
		deletions = merged.ToBuffer()
	}
	return Build(points, deletions), nil
}
