//go:build !cuvs

package cuvs_index

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

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

type VectorIndex interface {
	Add(id uint64, vector []float32) error
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	Delete(id ...uint64) error
	SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
}

type cuvs_index struct {
}

func New(cfg Config) (*cuvs_index, error) {
	return &cuvs_index{}, nil
}

func (index *cuvs_index) Add(id uint64, vector []float32) error {
	return nil
}

func (index *cuvs_index) AddBatch(ctx context.Context, id []uint64, vector [][]float32) error {
	return nil
}

func (index *cuvs_index) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, nil
}
