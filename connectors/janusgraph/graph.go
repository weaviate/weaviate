package janusgraph

import (
	"fmt"

	"github.com/graphql-go/graphql"
)

// GetGraph returns the result based on th graphQL request
func (f *Janusgraph) GetGraph(request graphql.ResolveParams) (interface{}, error) {
	return nil, fmt.Errorf("not supported")
}

// GraphqlListThings returns a list of things, similar to
// ListThings (REST API) but takes a graphql.ResolveParams to control
func (f *Janusgraph) GraphqlListThings(request graphql.ResolveParams) (interface{}, error) {
	return []map[string]string{{"name": "superbestname"}}, nil
}
