package network_get

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/creativesoftwarefdn/weaviate/models"
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
	return fmt.Sprintf("{ Local { %s } }", s)
}

// ProxyGetInstanceParams ties a SubQuery and a single instance
// together
type ProxyGetInstanceParams struct {
	SubQuery       SubQuery
	TargetInstance string
	Principal      *models.KeyTokenGetResponse
}

type Resolver interface {
	ProxyGetInstance(info ProxyGetInstanceParams) (*models.GraphQLResponse, error)
}

// FiltersAndResolver is a helper tuple to bubble data through the resolvers.
type FiltersAndResolver struct {
	Filters  FiltersPerInstance
	Resolver Resolver
}

func NetworkGetInstanceResolve(p graphql.ResolveParams) (interface{}, error) {
	filterAndResolver, ok := p.Source.(FiltersAndResolver)
	if !ok {
		return nil, fmt.Errorf("expected source to be a FilterAndResolver, but was \n%#v",
			p.Source)
	}

	resolver := filterAndResolver.Resolver
	filters := filterAndResolver.Filters
	astLoc := p.Info.FieldASTs[0].GetLoc()
	rawSubQuery := astLoc.Source.Body[astLoc.Start:astLoc.End]
	subQueryWithoutInstance, err := replaceInstanceNameAndInjectFilter(p.Info.FieldName, rawSubQuery, filters)
	if err != nil {
		return nil, fmt.Errorf("could not replace instance name in sub-query: %s", err)
	}

	principal, ok := p.Context.Value("principal").(*models.KeyTokenGetResponse)
	if !ok {
		return nil, fmt.Errorf("expected Context.Principal to be a KeyTokenGetResponse, but was %#v",
			p.Context.Value("principal"))
	}

	params := ProxyGetInstanceParams{
		SubQuery:       ParseSubQuery(subQueryWithoutInstance),
		TargetInstance: p.Info.FieldName,
		Principal:      principal,
	}

	graphQLResponse, err := resolver.ProxyGetInstance(params)
	if err != nil {
		return nil, fmt.Errorf("could not proxy to remote instance: %s", err)
	}

	local, ok := graphQLResponse.Data["Local"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected response.data.Local to be map[string]interface{}, but response was %#v",
			graphQLResponse.Data["Local"])
	}

	return local["Get"], nil
}

func replaceInstanceNameAndInjectFilter(instanceName string, query []byte, filters FiltersPerInstance) ([]byte, error) {
	r, err := regexp.Compile(fmt.Sprintf(`^%s\s*`, instanceName))
	if err != nil {
		return []byte{}, err
	}

	queryWithoutFilters := func() []byte { return r.ReplaceAll(query, []byte("Get ")) }
	if filters == nil {
		return queryWithoutFilters(), nil
	}

	instanceFilters, ok := filters[instanceName]
	if !ok {
		return queryWithoutFilters(), nil
	}

	where, ok := instanceFilters.(map[string]interface{})["where"]
	if !ok {
		return queryWithoutFilters(), nil
	}

	whereBytes, err := json.Marshal(where)
	if err != nil {
		return nil, fmt.Errorf("could not marshal extracted filters back to json: %s", err)
	}

	graphQLWhere := jsonToGraphQLWhere(whereBytes)
	return r.ReplaceAll(query, []byte(fmt.Sprintf("Get(where: %s) ", graphQLWhere))), nil
}

func jsonToGraphQLWhere(whereJSON []byte) []byte {
	// Remove Quotes on all keys, because graphql object structure
	// is more like javascript objects, then JSON. Quotes on keys
	// are not valid.
	replaceKeyQuotes := regexp.MustCompile(`"(\w+)"\s*:`)
	withoutKeyQuotes := replaceKeyQuotes.ReplaceAll(whereJSON, []byte("$1:"))

	// Remove quotes on operator value, because that's an Enum in the graphQL
	// schema, so it also doesn't expect quotes.
	replaceOperatorEnumQuotes := regexp.MustCompile(`operator:\s*"(\w+)"`)
	return replaceOperatorEnumQuotes.ReplaceAll(withoutKeyQuotes, []byte("operator:$1"))
}
