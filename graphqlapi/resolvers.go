package graphqlapi

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/go-openapi/strfmt"
	"github.com/graphql-go/graphql"
)

type localGetFilters struct {
}

// The data in a resolved class
type resolvedClass struct {
	kind      kind.Kind
	className schema.ClassName
	uuid      strfmt.UUID

	properties map[string]resolvedProperty
}

type resolvedProperty interface{}
type resolvePromise func() (interface{}, error)

type resolver interface {
	ResolveGetClass(info *getClassParams) (resolvePromise, error)
}

type pagination struct {
	first int
	after int
}

type property struct {
	name string
}

type getClassParams struct {
	filters    *localGetFilters
	kind       kind.Kind
	className  string
	pagination *pagination
	properties []property
}

func extractLocalGetFilters(p graphql.ResolveParams) (*localGetFilters, error) {
	//p.Args
	filters := localGetFilters{}

	return &filters, nil
}
