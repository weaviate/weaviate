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
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

func (s *Shard) analyzeObject(object *storobj.Object) ([]inverted.Property, []string, error) {
	schemaModel := s.index.getSchema.GetSchemaSkipAuth().Objects
	c, err := schema.GetClassByName(schemaModel, object.Class().String())
	if err != nil {
		return nil, nil, err
	}

	var schemaMap map[string]interface{}

	if object.Properties() == nil {
		schemaMap = make(map[string]interface{})
	} else {
		maybeSchemaMap, ok := object.Properties().(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("expected schema to be map, but got %T", object.Properties())
		}
		schemaMap = maybeSchemaMap
	}

	// add nil for all properties that are not part of the object so that they can be added to the inverted index for
	// the null state (if enabled)
	var nilProps []string
	if s.index.invertedIndexConfig.IndexNullState {
		for _, prop := range c.Properties {
			_, ok := schemaMap[prop.Name]
			if !ok {
				nilProps = append(nilProps, prop.Name)
			}
		}
	}

	if s.index.invertedIndexConfig.IndexTimestamps {
		if schemaMap == nil {
			schemaMap = make(map[string]interface{})
		}
		schemaMap[filters.InternalPropCreationTimeUnix] = object.Object.CreationTimeUnix
		schemaMap[filters.InternalPropLastUpdateTimeUnix] = object.Object.LastUpdateTimeUnix
	}

	props, err := inverted.NewAnalyzer(s.index.stopwords).Object(schemaMap, c.Properties, object.ID())
	return props, nilProps, err
}
