/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package meta

import (
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (b *Query) crefProp(prop traverser.MetaProperty) (*gremlin.Query, error) {
	for _, analysis := range prop.StatisticalAnalyses {
		if analysis != traverser.Count {
			continue
		}

		return b.crefCountQuery(prop), nil
	}
	return nil, nil
}

func (b *Query) crefCountQuery(prop traverser.MetaProperty) *gremlin.Query {
	return gremlin.New().
		OutEWithLabel(b.mappedPropertyName(b.params.ClassName, untitle(prop.Name))).Count().
		Project("count").Project(string(prop.Name))
}
