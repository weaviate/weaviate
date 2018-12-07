package network_get

import (
	"fmt"
	"regexp"

	"github.com/graphql-go/graphql"
)

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
// TODO: At the moment ignores filter params
func (s SubQuery) WrapInLocalQuery() string {
	return fmt.Sprintf("Local { %s }", s)
}

// ProxyGetInstanceParams ties a SubQuery and a single instance
// together
type ProxyGetInstanceParams struct {
	SubQuery       SubQuery
	TargetInstance string
}

type Resolver interface {
	ProxyGetInstance(info ProxyGetInstanceParams) (interface{}, error)
}

func NetworkGetInstanceResolve(p graphql.ResolveParams) (interface{}, error) {
	resolver := p.Source.(map[string]interface{})["Resolver"].(Resolver)
	astLoc := p.Info.FieldASTs[0].GetLoc()
	rawSubQuery := astLoc.Source.Body[astLoc.Start:astLoc.End]
	subQueryWithoutInstance, err := replaceInstanceName(p.Info.FieldName, rawSubQuery)
	if err != nil {
		return nil, fmt.Errorf("could not replace instance name in sub-query: %s", err)
	}

	params := ProxyGetInstanceParams{
		SubQuery:       ParseSubQuery(subQueryWithoutInstance),
		TargetInstance: p.Info.FieldName,
	}
	resolver.ProxyGetInstance(params)

	return nil, nil
}

func replaceInstanceName(instanceName string, query []byte) ([]byte, error) {
	r, err := regexp.Compile(fmt.Sprintf(`^%s\s*`, instanceName))
	if err != nil {
		return []byte{}, err
	}

	return r.ReplaceAll(query, []byte("Get ")), nil

}
