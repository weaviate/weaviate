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

package roaringsetrange

import (
	"context"
	"fmt"
	"math"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
)

type SegmentReader struct {
	cursor SegmentCursor
}

// TODO aliszka:roaringrange add buf pool?
func NewSegmentReader(cursor *GaplessSegmentCursor) *SegmentReader {
	return &SegmentReader{cursor: cursor}
}

func (r *SegmentReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, func(), error) {
	if err := ctx.Err(); err != nil {
		return roaringset.BitmapLayer{}, noopRelease, err
	}

	// conc is the per-query merge budget, threaded through every read/merge helper below.
	conc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)

	switch operator {
	case filters.OperatorEqual:
		bm, err := r.readEqual(ctx, value, conc)
		return bm, noopRelease, err

	case filters.OperatorNotEqual:
		bm, err := r.readNotEqual(ctx, value, conc)
		return bm, noopRelease, err

	case filters.OperatorLessThan:
		bm, err := r.readLessThan(ctx, value, conc)
		return bm, noopRelease, err

	case filters.OperatorLessThanEqual:
		bm, err := r.readLessThanEqual(ctx, value, conc)
		return bm, noopRelease, err

	case filters.OperatorGreaterThan:
		bm, err := r.readGreaterThan(ctx, value, conc)
		return bm, noopRelease, err

	case filters.OperatorGreaterThanEqual:
		bm, err := r.readGreaterThanEqual(ctx, value, conc)
		return bm, noopRelease, err

	default:
		// TODO move strategies to separate package?
		return roaringset.BitmapLayer{}, noopRelease,
			fmt.Errorf("operator %v not supported for segments of strategy %q", operator.Name(), "roaringsetrange")
	}
}

// firstLayer returns layer 0's additions as the cursor's view, which a read
// must not modify. The pread cursor overwrites it with later nodes, so a read
// builds everything it needs from it before its merge calls Next().
func (r *SegmentReader) firstLayer() (roaringset.BitmapLayer, bool) {
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
		Additions: layer.Additions,
		Deletions: deletions,
	}, true
}

func (r *SegmentReader) readEqual(ctx context.Context, value uint64, conc int,
) (roaringset.BitmapLayer, error) {
	// max reads faster as GreaterThanEqual, and 0 through mergeEqual (BenchmarkSegmentReader).
	if value == math.MaxUint64 {
		return r.readGreaterThanEqual(ctx, value, conc)
	}

	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	eq, err := r.mergeEqual(ctx, value, firstLayer.Additions.Clone(), conc)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	return roaringset.BitmapLayer{
		Additions: eq,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readNotEqual(ctx context.Context, value uint64, conc int,
) (roaringset.BitmapLayer, error) {
	// max reads faster as LessThan, and 0 through mergeEqual (BenchmarkSegmentReader).
	if value == math.MaxUint64 {
		return r.readLessThan(ctx, value, conc)
	}

	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	neq := firstLayer.Additions.Clone()
	eq, err := r.mergeEqual(ctx, value, firstLayer.Additions.Clone(), conc)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	neq.AndNotConc(eq, conc)
	return roaringset.BitmapLayer{
		Additions: neq,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readLessThan(ctx context.Context, value uint64, conc int,
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
	gte, err := r.mergeGreaterThanEqual(ctx, value, firstLayer.Additions.ZeroedLayout(), conc)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	lt.AndNotConc(gte, conc)
	return roaringset.BitmapLayer{
		Additions: lt,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readLessThanEqual(ctx context.Context, value uint64, conc int,
) (roaringset.BitmapLayer, error) {
	// <= 0 is Equal 0, which reads faster through mergeEqual (BenchmarkSegmentReader).
	if value == 0 {
		return r.readEqual(ctx, value, conc)
	}

	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	if value == math.MaxUint64 {
		// all values are <= max uint64
		firstLayer.Additions = firstLayer.Additions.Clone()
		return firstLayer, nil
	}

	lte := firstLayer.Additions.Clone()
	gt, err := r.mergeGreaterThanEqual(ctx, value+1, firstLayer.Additions.ZeroedLayout(), conc)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	lte.AndNotConc(gt, conc)
	return roaringset.BitmapLayer{
		Additions: lte,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readGreaterThan(ctx context.Context, value uint64, conc int,
) (roaringset.BitmapLayer, error) {
	// > 0 is NotEqual 0, which reads faster through mergeEqual (BenchmarkSegmentReader).
	if value == 0 {
		return r.readNotEqual(ctx, value, conc)
	}

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

	gt, err := r.mergeGreaterThanEqual(ctx, value+1, firstLayer.Additions.ZeroedLayout(), conc)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	return roaringset.BitmapLayer{
		Additions: gt,
		Deletions: firstLayer.Deletions,
	}, nil
}

func (r *SegmentReader) readGreaterThanEqual(ctx context.Context, value uint64, conc int,
) (roaringset.BitmapLayer, error) {
	firstLayer, ok := r.firstLayer()
	if !ok {
		return firstLayer, nil
	}

	if value == 0 {
		// all values are >= 0
		firstLayer.Additions = firstLayer.Additions.Clone()
		return firstLayer, nil
	}

	gte, err := r.mergeGreaterThanEqual(ctx, value, firstLayer.Additions.ZeroedLayout(), conc)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	return roaringset.BitmapLayer{
		Additions: gte,
		Deletions: firstLayer.Deletions,
	}, nil
}

// mergeGreaterThanEqual fills result, an empty bitmap laid out like layer 0,
// with layer 0's rows whose value is >= value. value must be above 0.
func (r *SegmentReader) mergeGreaterThanEqual(ctx context.Context, value uint64,
	result *sroar.Bitmap, conc int,
) (*sroar.Bitmap, error) {
	merged := false
	for entry := range r.layers(ctx) {
		bitSet := value&(1<<(entry.bit-1)) != 0
		switch {
		case bitSet && !merged:
			// every layer is a subset of layer 0, so ORing it into the empty
			// result equals ANDing layer 0 with it
			result.OrConc(entry.layer.Additions, conc)
			merged = true
		case bitSet:
			result.AndConc(entry.layer.Additions, conc)
		case merged:
			result.OrConc(entry.layer.Additions, conc)
		}
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return result, nil
}

// mergeEqual keeps the rows of result that are in every layer of value's set
// bits and in no layer of its unset bits.
func (r *SegmentReader) mergeEqual(ctx context.Context, value uint64,
	result *sroar.Bitmap, conc int,
) (*sroar.Bitmap, error) {
	for entry := range r.layers(ctx) {
		if value&(1<<(entry.bit-1)) != 0 {
			result.AndConc(entry.layer.Additions, conc)
		} else {
			result.AndNotConc(entry.layer.Additions, conc)
		}
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return result, nil
}

// layers sends the cursor's bit layers after layer 0 until the cursor or ctx ends.
func (r *SegmentReader) layers(ctx context.Context) <-chan cursorEntry {
	entriesCh := make(chan cursorEntry)
	errors.GoWrapper(func() {
		defer close(entriesCh)
		for bit, layer, ok := r.cursor.Next(); ok; bit, layer, ok = r.cursor.Next() {
			if ctx.Err() != nil {
				break
			}
			entriesCh <- cursorEntry{bit: bit, layer: layer}
		}
	}, nil)
	return entriesCh
}

type cursorEntry struct {
	bit   uint8
	layer roaringset.BitmapLayer
}
