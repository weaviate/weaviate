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

package hnsw

import (
	"context"
	"encoding/binary"
	"math"
)

func idVectorSize(size int) func(ctx context.Context, id uint64) ([]float32, error) {
	return func(ctx context.Context, id uint64) ([]float32, error) {
		vector := make([]float32, size)
		for i := 0; i < size; i++ {
			vector[i] = float32(id)
		}
		return vector, nil
	}
}

func float32FromBytes(b []byte) float32 {
	bits := binary.LittleEndian.Uint32(b)
	return math.Float32frombits(bits)
}

func int32FromBytes(b []byte) int {
	return int(binary.LittleEndian.Uint32(b))
}
