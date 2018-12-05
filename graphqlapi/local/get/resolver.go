package local_get

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	common "github.com/creativesoftwarefdn/weaviate/graphqlapi/common_resolver"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

type Resolver interface {
	LocalGetClass(info *LocalGetClassParams) (func() interface{}, error)
}

type LocalGetClassParams struct {
	Kind       kind.Kind
	Filters    *common_filters.LocalFilter
	ClassName  string
	Pagination *common.Pagination
	Properties []SelectProperty
}

type SelectProperty struct {
	Name string

	IsPrimitive bool

	// Include the __typename in all the Refs below.
	IncludeTypeName bool

	// Not a primitive type? Then select these properties.
	Refs []SelectClass
}

// TODO move from ClassName and Properties to SelectClass in LocalGetClassParams.
type SelectClass struct {
	ClassName     string
	RefProperties []SelectProperty
}

// Internal struct to bubble data through the resolvers.
type filtersAndResolver struct {
	filters  *common_filters.LocalFilter
	resolver Resolver
}

func (sp SelectProperty) FindSelectClass(className schema.ClassName) *SelectClass {
	for _, selectClass := range sp.Refs {
		if selectClass.ClassName == string(className) {
			return &selectClass
		}
	}

	return nil
}
