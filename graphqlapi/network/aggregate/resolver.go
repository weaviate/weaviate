package aggregate

import (
	"fmt"

	"github.com/graphql-go/graphql"
)

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Network.Get queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}

func resolveClass(params graphql.ResolveParams) (interface{}, error) {
	// do logging here after resolvers for different aggregators are implemented (RequestsLog.Register())
	return nil, fmt.Errorf("resolve network aggregate class field not yet supported")
}
