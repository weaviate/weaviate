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

//go:build !cuvs

package cuvs_index

import (
	"context"
	"sync"

	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/rapidsai/cuvs/go/cagra"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	cuvsEnt "github.com/weaviate/weaviate/entities/vectorindex/cuvs"
)

type VectorIndex interface {
	// Dump(labels ...string)
	Add(id uint64, vector []float32) error
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	Delete(id ...uint64) error
	SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	// SearchByVectorDistance(vector []float32, dist float32,
	// 	maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	// UpdateUserConfig(updated schemaconfig.VectorIndexConfig, callback func()) error
	// Drop(ctx context.Context) error
	// Shutdown(ctx context.Context) error
	// Flush() error
	// SwitchCommitLogs(ctx context.Context) error
	// ListFiles(ctx context.Context, basePath string) ([]string, error)
	PostStartup()
	// Compressed() bool
	// ValidateBeforeInsert(vector []float32) error
	// DistanceBetweenVectors(x, y []float32) (float32, error)
	// ContainsNode(id uint64) bool
	// DistancerProvider() distancer.Provider
	AlreadyIndexed() uint64
	// QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer
}

type cuvs_index struct {
	sync.Mutex
	id              string
	targetVector    string
	dims            int32
	store           *lsmkv.Store
	logger          logrus.FieldLogger
	distanceMetric  cuvs.Distance
	cuvsIndex       *cagra.CagraIndex
	cuvsIndexParams *cagra.IndexParams
	dlpackTensor    *cuvs.Tensor[float32]
	idCuvsIdMap     map[uint32]uint64
	cuvsResource    *cuvs.Resource
	cuvsExtendCount uint64
	cuvsNumExtends  uint64

	// rescore             int64
	// bq                  compressionhelpers.BinaryQuantizer

	// pqResults *common.PqMaxPool
	// pool      *pools

	// compression string
	// bqCache     cache.Cache[uint64]
	count uint64
}

func New(cfg Config, uc cuvsEnt.UserConfig, store *lsmkv.Store) (*cuvs_index, error) {
	// Stub implementation
	return &cuvs_index{}, nil
}

func (index *cuvs_index) Add(id uint64, vector []float32) error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) AddBatch(ctx context.Context, id []uint64, vector [][]float32) error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) Delete(ids ...uint64) error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	// Stub implementation
	return []uint64{}, []float32{}, nil
}

func (index *cuvs_index) SearchByVectorBatch(vector [][]float32, k int, allow helpers.AllowList) ([][]uint64, [][]float32, error) {
	// Stub implementation
	return [][]uint64{}, [][]float32{}, nil
}

func (index *cuvs_index) PostStartup() {
	// Stub implementation
}

func (index *cuvs_index) AlreadyIndexed() uint64 {
	// Stub implementation
	return 0
}

func (index *cuvs_index) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error) {
	// Stub implementation
	return []uint64{}, []float32{}, nil
}

func (index *cuvs_index) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) Drop(ctx context.Context) error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) Shutdown(ctx context.Context) error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) Flush() error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) SwitchCommitLogs(ctx context.Context) error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	// Stub implementation
	return []string{}, nil
}

func (index *cuvs_index) Compressed() bool {
	// Stub implementation
	return false
}

func (index *cuvs_index) ValidateBeforeInsert(vector []float32) error {
	// Stub implementation
	return nil
}

func (index *cuvs_index) DistanceBetweenVectors(x, y []float32) (float32, error) {
	// Stub implementation
	return 0, nil
}

func (index *cuvs_index) ContainsNode(id uint64) bool {
	// Stub implementation
	return false
}

func (index *cuvs_index) DistancerProvider() distancer.Provider {
	// Stub implementation
	return nil
}

func (index *cuvs_index) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	// Stub implementation
	return common.QueryVectorDistancer{}
}

func (index *cuvs_index) Dump(labels ...string) {
}
