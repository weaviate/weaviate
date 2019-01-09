package filters

import "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"

// Query from filter params. Can be appended to any GremlinQuery
type Query struct{}

func (q *Query) String() string {
	return ""
}

// New Query from local filter params
func New(filters *common_filters.LocalFilter) *Query {
	return &Query{}
}
