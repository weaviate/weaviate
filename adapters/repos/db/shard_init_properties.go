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
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (s *Shard) initProperties() error {
	s.propertyIndices = propertyspecific.Indices{}
	sch := s.index.getSchema.GetSchemaSkipAuth()
	c := sch.FindClassByName(s.index.Config.ClassName)
	if c == nil {
		return nil
	}

	for _, prop := range c.Properties {
		if prop.IndexInverted != nil && !*prop.IndexInverted {
			continue
		}

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
	}

	if err := s.addIDProperty(context.TODO()); err != nil {
		return errors.Wrap(err, "init id property")
	}

	if s.index.invertedIndexConfig.IndexTimestamps {
		if err := s.addTimestampProperties(context.TODO()); err != nil {
			return errors.Wrap(err, "init timestamp properties")
		}
	}

	return nil
}
