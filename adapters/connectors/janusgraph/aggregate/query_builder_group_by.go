//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func (b *Query) groupByQuery() *gremlin.Query {
	if b.params.GroupBy.Child == nil {
		return b.groupByPrimitiveProperty()
	}

	return b.groupByReferenceProperty()
}

func (b *Query) groupByPrimitiveProperty() *gremlin.Query {
	return gremlin.New().
		Group().
		By(b.mappedPropertyName(b.params.GroupBy.Class, b.params.GroupBy.Property))
}

func (b *Query) groupByReferenceProperty() *gremlin.Query {
	edgePath := b.buildEdgePath(b.params.GroupBy)

	return gremlin.New().
		Group().
		ByQuery(edgePath)
}

func (b *Query) buildEdgePath(path *filters.Path) *gremlin.Query {
	q := gremlin.New()
	edgeLabel := b.mappedPropertyName(path.Class, path.Property)
	referencedClass := b.mappedClassName(path.Child.Class)
	q = q.OutWithLabel(edgeLabel).
		HasString("classId", referencedClass)

	if path.Child.Child == nil {
		// the child clause doesn't have any more children this means we have
		// reached the end and can now append the values query
		return q.Values([]string{b.mappedPropertyName(path.Child.Class, path.Child.Property)})
	}

	// Since there are more children, we need to go deeper
	childEdgePath := b.buildEdgePath(path.Child)

	return q.Raw(fmt.Sprintf(".%s", childEdgePath.String()))
}
