package local_get

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	common "github.com/creativesoftwarefdn/weaviate/graphqlapi/common_resolver"
	common_local "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_resolver"
)

type Resolver interface {
	LocalGetClass(info *LocalGetClassParams) (func() interface{}, error)
}

type LocalGetClassParams struct {
	Filters    *common_local.LocalFilters
	Kind       kind.Kind
	ClassName  string
	Pagination *common.Pagination
	Properties []SelectProperty
}

type SelectProperty struct {
	Name string
}

// Internal struct to bubble data through the resolvers.
type filtersAndResolver struct {
	filters  *common_local.LocalFilters
	resolver Resolver
}
