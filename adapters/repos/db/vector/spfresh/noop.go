//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

func (s *SPFresh) AddMulti(ctx context.Context, docId uint64, vector [][]float32) error {
	return errors.New("AddMulti is not supported for the spfresh index")
}

func (s *SPFresh) AddMultiBatch(ctx context.Context, docIds []uint64, vectors [][][]float32) error {
	return errors.New("AddMultiBatch is not supported for the spfresh index")
}

func (s *SPFresh) DeleteMulti(id ...uint64) error {
	return errors.New("DeleteMulti is not supported for the spfresh index")
}

func (s *SPFresh) SearchByMultiVector(ctx context.Context, vector [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.New("SearchByMultiVector is not supported for the spfresh index")
}

func (s *SPFresh) SearchByMultiVectorDistance(ctx context.Context, vector [][]float32, dist float32,
	maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.New("SearchByMultiVectorDistance is not supported for the spfresh index")
}

func (s *SPFresh) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	return errors.New("UpdateUserConfig is not supported for the spfresh index")
}

func (s *SPFresh) ValidateMultiBeforeInsert(vector [][]float32) error {
	return errors.New("ValidateMultiBeforeInsert is not supported for the spfresh index")
}

func (s *SPFresh) QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer {
	panic("QueryMultiVectorDistancer is not supported for the spfresh index")
}
