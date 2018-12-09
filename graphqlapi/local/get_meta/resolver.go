package local_get_meta

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

type Resolver interface {
	LocalGetMeta(info *LocalGetMetaParams) (func() interface{}, error)
}

type LocalGetMetaParams struct {
	Kind             kind.Kind
	Filters          *common_filters.LocalFilter
	ClassName        string
	Properties       []MetaProperty
	includeMetaCount bool
}

type MetaProperty struct {
	Name string
	Type string
}
