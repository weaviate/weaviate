/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package meta

import (
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func (b *Query) crefProp(prop kinds.MetaProperty) (*gremlin.Query, error) {
	for _, analysis := range prop.StatisticalAnalyses {
		if analysis != kinds.Count {
			continue
		}

		return b.crefCountQuery(prop), nil
	}
	return nil, nil
}

func (b *Query) crefCountQuery(prop kinds.MetaProperty) *gremlin.Query {
	return gremlin.New().
		OutEWithLabel(b.mappedPropertyName(b.params.ClassName, untitle(prop.Name))).Count().
		Project("count").Project(string(prop.Name))
}
