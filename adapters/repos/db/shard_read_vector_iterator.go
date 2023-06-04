//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) newVectorIterator() hnsw.VectorIterator[float32] {
	c := s.store.Bucket(helpers.ObjectsBucketLSM).Cursor()
	return &vectorIterator{c: c}
}

type vectorIterator struct {
	c           *lsmkv.CursorReplace
	initialized bool
}

func (vi *vectorIterator) Close() {
	vi.c.Close()
}

func (vi *vectorIterator) Next() ([]float32, uint64, error) {
	var k, v []byte
	if vi.initialized {
		k, v = vi.c.Next()
	} else {
		k, v = vi.c.First()
		vi.initialized = true
	}

	if k == nil {
		return nil, 0, nil
	}

	id, vec, err := storobj.DocIDAndVectorFromBinary(v)
	return vec, id, err
}
