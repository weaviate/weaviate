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
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (s *Shard) analyzeObject(object *storobj.Object) ([]inverted.Property, error) {
	if object.Properties() == nil {
		return nil, nil
	}

	schemaModel := s.index.getSchema.GetSchemaSkipAuth().Objects
	c, err := schema.GetClassByName(schemaModel, object.Class().String())
	if err != nil {
		return nil, err
	}

	schemaMap, ok := object.Properties().(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected schema to be map, but got %T", object.Properties())
	}

	if s.index.invertedIndexConfig.IndexTimestamps {
		if schemaMap == nil {
			schemaMap = make(map[string]interface{})
		}
		schemaMap[traverser.InternalPropCreationTimeUnix] = object.Object.CreationTimeUnix
		schemaMap[traverser.InternalPropLastUpdateTimeUnix] = object.Object.LastUpdateTimeUnix
	}

	return inverted.NewAnalyzer(s.index.stopwords).Object(schemaMap, c.Properties, object.ID())
}
