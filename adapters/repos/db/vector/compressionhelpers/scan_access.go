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

package compressionhelpers

import (
	"context"

	"github.com/pkg/errors"
)

// WordCodeSource exposes stored word codes and the quantizer that wrote
// them, for scan paths that iterate codes directly (the filtered prefix
// scan) instead of going through per-pair distancers. Implemented by the
// quantized compressors; only meaningful when the code element type is
// uint64 (1-bit RQ families).
type WordCodeSource interface {
	// WordCode returns the stored code of id as words. The returned slice
	// may be a view into cache memory (arena caches): treat it as read-only
	// and do not retain it across writes to the same id.
	WordCode(ctx context.Context, id uint64) ([]uint64, error)
	// ScanQuantizer returns the underlying quantizer for type assertion by
	// the scan path.
	ScanQuantizer() any
}

func (compressor *quantizedVectorsCompressor[T]) WordCode(ctx context.Context, id uint64) ([]uint64, error) {
	code, err := compressor.cache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if words, ok := any(code).([]uint64); ok {
		return words, nil
	}
	return nil, errors.Errorf("compressor stores %T codes, not word codes", code)
}

func (compressor *quantizedVectorsCompressor[T]) ScanQuantizer() any {
	return compressor.quantizer
}
