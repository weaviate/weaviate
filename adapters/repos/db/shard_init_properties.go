//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/propertyspecific"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"golang.org/x/sync/errgroup"
)

func (s *Shard) initProperties() error {
	s.propertyIndices = propertyspecific.Indices{}
	sch := s.index.getSchema.GetSchemaSkipAuth()
	c := sch.FindClassByName(s.index.Config.ClassName)
	if c == nil {
		return nil
	}

	eg := &errgroup.Group{}
	for _, prop := range c.Properties {
		if prop.IndexInverted != nil && !*prop.IndexInverted {
			continue
		}

		// Important: wrap the error group goroutine spawning in a new closure,
		// otherwise it is entirely unpredictable what "prop" refers to in each new
		// goroutine. This is because we are spawning new goroutines from a loop.
		// For details, see this excellent blog post:
		// https://appliedgo.com/blog/goroutine-closures
		func(prop *models.Property) {
			eg.Go(func() error {
				if schema.DataType(prop.DataType[0]) == schema.DataTypeGeoCoordinates {
					if err := s.initGeoProp(prop); err != nil {
						return errors.Wrapf(err, "init property %s", prop.Name)
					}
				} else {
					// served by the inverted index, init the buckets there
					if err := s.addProperty(context.TODO(), prop); err != nil {
						return errors.Wrapf(err, "init property %s", prop.Name)
					}
				}
				if s.index.invertedIndexConfig.IndexNullState {
					eg.Go(func() error {
						if err := s.addNullState(context.TODO(), prop); err != nil {
							return errors.Wrapf(err, "init property %s null state", prop.Name)
						}

						return nil
					})
				}
				if s.index.invertedIndexConfig.IndexPropertyLength {
					eg.Go(func() error {
						if err := s.addPropertyLength(context.TODO(), prop); err != nil {
							return errors.Wrapf(err, "init property %s null state", prop.Name)
						}

						return nil
					})
				}
				return nil
			})
		}(prop)
	}

	eg.Go(func() error {
		if err := s.addIDProperty(context.TODO()); err != nil {
			return errors.Wrap(err, "init id property")
		}

		return nil
	})

	if s.index.invertedIndexConfig.IndexTimestamps {
		eg.Go(func() error {
			if err := s.addTimestampProperties(context.TODO()); err != nil {
				return errors.Wrap(err, "init timestamp properties")
			}

			return nil
		})
	}

	return eg.Wait()
}
