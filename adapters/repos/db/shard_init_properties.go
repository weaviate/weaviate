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

package db

import (
	"context"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/models"
)

func (s *Shard) initProperties(eg *enterrors.ErrorGroupWrapper, class *models.Class) {
	s.propertyIndices = propertyspecific.Indices{}
	if class == nil {
		return
	}

	s.initPropertyBuckets(context.Background(), eg, class.Properties...)

	eg.Go(func() error {
		return s.addIDProperty(context.TODO())
	})

	if s.index.invertedIndexConfig.IndexTimestamps {
		eg.Go(func() error {
			return s.addTimestampProperties(context.TODO())
		})
	}

	if s.index.Config.TrackVectorDimensions {
		eg.Go(func() error {
			return s.addDimensionsProperty(context.TODO())
		})
	}
}
