package meta

import (
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

func (b *Query) crefProp(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	for _, analysis := range prop.StatisticalAnalyses {
		if analysis != getmeta.Count {
			continue
		}

		q := gremlin.New().
			Union(b.crefCountQuery(prop)).
			AsProjectBy(string(prop.Name))

		return q, nil
	}
	return nil, nil
}

func (b *Query) crefCountQuery(prop getmeta.MetaProperty) *gremlin.Query {
	return gremlin.New().
		OutEWithLabel(b.mappedPropertyName(b.params.ClassName, untitle(prop.Name))).Count().
		AsProjectBy("refcount", "count")
}
