//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringsetrange

import (
	"context"
	"fmt"
	"math"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
)

type SegmentReader struct {
	cursor      SegmentCursor
	concurrency int
}

func NewSegmentReader(cursor *GaplessSegmentCursor) *SegmentReader {
	return NewSegmentReaderConcurrent(cursor, 1)
}

// TODO aliszka:roaringrange add buf pool?
func NewSegmentReaderConcurrent(cursor *GaplessSegmentCursor, concurrency int) *SegmentReader {
	return &SegmentReader{
		cursor:      cursor,
		concurrency: concurrency,
	}
}

func (r *SegmentReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, func(), error) {
	if err := ctx.Err(); err != nil {
		return roaringset.BitmapLayer{}, noopRelease, err
	}

	switch operator {
	case filters.OperatorEqual:
		bm, err := r.readEqual(ctx, value)
		return bm, noopRelease, err

	case filters.OperatorNotEqual:
		bm, err := r.readNotEqual(ctx, value)
		return bm, noopRelease, err

	case filters.OperatorLessThan:
		bm, err := r.readLessThan(ctx, value)
		return bm, noopRelease, err

	case filters.OperatorLessThanEqual:
		bm, err := r.readLessThanEqual(ctx, value)
		return bm, noopRelease, err

	case filters.OperatorGreaterThan:
		bm, err := r.readGreaterThan(ctx, value)
		return bm, noopRelease, err

	case filters.OperatorGreaterThanEqual:
		bm, err := r.readGreaterThanEqual(ctx, value)
		return bm, noopRelease, err

	default:
		// TODO move strategies to separate package?
		return roaringset.BitmapLayer{}, noopRelease,
			fmt.Errorf("operator %v not supported for segments of strategy %q", operator.Name(), "roaringsetrange")
	}
}

func (r *SegmentReader) firstLayer() (roaringset.BitmapLayer, bool) {
	// bitmaps' cloning is necessary for both types of cursors: mmap and pread
	// (pread cursor use buffers to read entire nodes from file, therefore nodes already read
	// are later overwritten with nodes being read later)
	_, layer, ok := r.cursor.First()
	if !ok {
		return roaringset.BitmapLayer{
			Additions: sroar.NewBitmap(),
			Deletions: sroar.NewBitmap(),
		}, false
	}

	var deletions *sroar.Bitmap
	if layer.Deletions == nil {
		deletions = sroar.NewBitmap()
	} else {
		deletions = layer.Deletions.Clone()
	}

	if layer.Additions.IsEmpty() {
		return roaringset.BitmapLayer{
			Additions: sroar.NewBitmap(),
			Deletions: deletions,
		}, false
	}
	return roaringset.BitmapLayer{
		Additions: layer.Additions.Clone(),
		Deletions: deletions,
	}, true
}

func (r *SegmentReader) readEqual(ctx context.Context, value uint64,
) (roaringset.BitmapLayer, error) {
	if value == 0 {
		return r.readLessThanEqual(ctx, value)
	}
	if value == math.MaxUint64 {
		return r.readGreaterThanEqual(ctx, value)
	}

	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	eq, err := r.mergeBetween(ctx, value, value+1, firstLayer.Additions)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	return roaringset.BitmapLayer{
		Additions: eq,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readNotEqual(ctx context.Context, value uint64,
) (roaringset.BitmapLayer, error) {
	if value == 0 {
		return r.readGreaterThan(ctx, value)
	}
	if value == math.MaxUint64 {
		return r.readLessThan(ctx, value)
	}

	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	neq := firstLayer.Additions.Clone()
	eq, err := r.mergeBetween(ctx, value, value+1, firstLayer.Additions)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	neq.AndNotConc(eq, r.concurrency)
	return roaringset.BitmapLayer{
		Additions: neq,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readLessThan(ctx context.Context, value uint64,
) (roaringset.BitmapLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	if value == 0 {
		// no value is < 0
		return roaringset.BitmapLayer{
			Additions: sroar.NewBitmap(),
			Deletions: firstLayer.Deletions,
		}, nil
	}

	lt := firstLayer.Additions.Clone()
	gte, err := r.mergeGreaterThanEqual(ctx, value, firstLayer.Additions)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	lt.AndNotConc(gte, r.concurrency)
	return roaringset.BitmapLayer{
		Additions: lt,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readLessThanEqual(ctx context.Context, value uint64,
) (roaringset.BitmapLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	if value == math.MaxUint64 {
		// all values are <= max uint64
		return firstLayer, nil
	}

	lte := firstLayer.Additions.Clone()
	gte1, err := r.mergeGreaterThanEqual(ctx, value+1, firstLayer.Additions)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	lte.AndNotConc(gte1, r.concurrency)
	return roaringset.BitmapLayer{
		Additions: lte,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readGreaterThan(ctx context.Context, value uint64,
) (roaringset.BitmapLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	if value == math.MaxUint64 {
		// no value is > max uint64
		return roaringset.BitmapLayer{
			Additions: sroar.NewBitmap(),
			Deletions: firstLayer.Deletions,
		}, nil
	}

	gte1, err := r.mergeGreaterThanEqual(ctx, value+1, firstLayer.Additions)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	return roaringset.BitmapLayer{
		Additions: gte1,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readGreaterThanEqual(ctx context.Context, value uint64,
) (roaringset.BitmapLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	gte, err := r.mergeGreaterThanEqual(ctx, value, firstLayer.Additions)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	return roaringset.BitmapLayer{
		Additions: gte,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) mergeGreaterThanEqual(ctx context.Context, value uint64,
	all *sroar.Bitmap,
) (*sroar.Bitmap, error) {
	ANDed := false
	result := all
	entriesCh := make(chan *cursorEntry)

	errors.GoWrapper(func() {
		defer close(entriesCh)
		for bit, layer, ok := r.cursor.Next(); ok; bit, layer, ok = r.cursor.Next() {
			if ctx.Err() != nil {
				break
			}
			entriesCh <- &cursorEntry{bit: bit, layer: layer}
		}
	}, nil)

	for entry := range entriesCh {
		bit := entry.bit
		layer := entry.layer

		if value&(1<<(bit-1)) != 0 {
			ANDed = true
			result.AndConc(layer.Additions, r.concurrency)
		} else if ANDed {
			result.OrConc(layer.Additions, r.concurrency)
		}
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return result, nil
}

func (r *SegmentReader) mergeBetween(ctx context.Context, valueMinInc, valueMaxExc uint64,
	all *sroar.Bitmap,
) (*sroar.Bitmap, error) {
	ANDedMin := false
	ANDedMax := false
	resultMin := all.Clone()
	resultMax := all
	entriesCh := make(chan *cursorEntry)

	errors.GoWrapper(func() {
		defer close(entriesCh)
		for bit, layer, ok := r.cursor.Next(); ok; bit, layer, ok = r.cursor.Next() {
			if ctx.Err() != nil {
				break
			}
			entriesCh <- &cursorEntry{bit: bit, layer: layer}
		}
	}, nil)

	for entry := range entriesCh {
		bit := entry.bit
		layer := entry.layer

		var b uint64 = 1 << (bit - 1)

		if valueMinInc&b != 0 {
			ANDedMin = true
			resultMin.AndConc(layer.Additions, r.concurrency)
		} else if ANDedMin {
			resultMin.OrConc(layer.Additions, r.concurrency)
		}

		if valueMaxExc&b != 0 {
			ANDedMax = true
			resultMax.AndConc(layer.Additions, r.concurrency)
		} else if ANDedMax {
			resultMax.OrConc(layer.Additions, r.concurrency)
		}
	}

	resultMin.AndNotConc(resultMax, r.concurrency)

	return resultMin, nil
}

type cursorEntry struct {
	bit   uint8
	layer roaringset.BitmapLayer
}
