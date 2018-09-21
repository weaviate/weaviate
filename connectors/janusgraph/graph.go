package janusgraph

import (
	"fmt"

	"github.com/graphql-go/graphql"
)

// GetGraph returns the result based on th graphQL request
func (f *Janusgraph) GetGraph(request graphql.ResolveParams) (interface{}, error) {
	return nil, fmt.Errorf("not supported")
}
