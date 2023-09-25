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
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/models"
	"golang.org/x/sync/errgroup"
)

func (s *Shard) initProperties(class *models.Class) error {
	if !lsmkv.FeatureUseMergedBuckets {
		return s.initProperties_old(class)
	}
	s.propertyIndices = propertyspecific.Indices{}
	if class == nil {
		return nil
	}

	for _, prop := range class.Properties {
		s.createPropertyIndex(context.TODO(), prop)
	}

	if err := s.addIDProperty(context.TODO()); err != nil {
		return errors.Wrap(err, "create id property index")
	}

	if s.index.invertedIndexConfig.IndexTimestamps {
		if err := s.addTimestampProperties(context.TODO()); err != nil {
			return errors.Wrap(err, "create timestamp properties indexes")
		}
	}

	if s.index.Config.TrackVectorDimensions {
		if err := s.addDimensionsProperty(context.TODO()); err != nil {
			return errors.Wrap(err, "crreate dimensions property index")
		}
	}

	return nil
}

func (s *Shard) initProperties_old(class *models.Class) error {
	s.propertyIndices = propertyspecific.Indices{}
	if class == nil {
		return nil
	}

	eg := &errgroup.Group{}
	for _, prop := range class.Properties {
		s.createPropertyIndex_old(context.TODO(), prop, eg)
	}

	eg.Go(func() error {
		if err := s.addIDProperty_old(context.TODO()); err != nil {
			return errors.Wrap(err, "create id property index")
		}
		return nil
	})

	if s.index.invertedIndexConfig.IndexTimestamps {
		eg.Go(func() error {
			if err := s.addTimestampProperties_old(context.TODO()); err != nil {
				return errors.Wrap(err, "create timestamp properties indexes")
			}
			return nil
		})
	}

	if s.index.Config.TrackVectorDimensions {
		eg.Go(func() error {
			if err := s.addDimensionsProperty_old(context.TODO()); err != nil {
				return errors.Wrap(err, "crreate dimensions property index")
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "init properties on shard '%s'", s.ID())
	}
	return nil
}
