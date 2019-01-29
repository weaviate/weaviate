package common

import "fmt"

// SubQuery is an extracted query from the Network Query,
// it is intended for exactly one target instance and is
// formatted in a way where it can easily be transformed
// into a Local Query to be used with the remote instance's
// GraphQL API
type SubQuery string

// ParseSubQuery from a []byte
func ParseSubQuery(subQuery []byte) SubQuery {
	return SubQuery(string(subQuery))
}

// WrapInLocalQuery assumes the subquery can be sent as part of a
// Local-Query, i.e. it should start with `Get{ ... }`
func (s SubQuery) WrapInLocalQuery() string {
	return fmt.Sprintf("{ Local { %s } }", s)
}
