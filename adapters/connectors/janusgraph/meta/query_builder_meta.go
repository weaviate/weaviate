//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package meta

import (
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

const (
	// MetaProp ("meta") is special in that it can be used an any class
	// regardless of the the actual properties, to retrieve generic information
	// such as the count of all class instances.
	MetaProp schema.PropertyName = "meta"
)

func (b *Query) metaProp(prop traverser.MetaProperty) (*gremlin.Query, error) {
	if len(prop.StatisticalAnalyses) != 1 {
		return nil, fmt.Errorf(
			"meta prop only supports exactly one statistical analysis prop 'count', but have: %#v",
			prop.StatisticalAnalyses)
	}

	analysis := prop.StatisticalAnalyses[0]
	if analysis != traverser.Count {
		return nil, fmt.Errorf(
			"meta prop only supports statistical analysis prop 'count', but have '%s'", analysis)
	}

	return b.metaCountQuery(), nil
}

func (b *Query) metaCountQuery() *gremlin.Query {
	return gremlin.New().
		Count().
		Project("count").
		Project("meta")
}
