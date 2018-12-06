package network_get

import (
	"fmt"
	"regexp"

	"github.com/creativesoftwarefdn/weaviate/network"
	"github.com/graphql-go/graphql"
)

type Resolver interface {
	ProxyGetInstance(info network.ProxyGetInstanceParams) (func() interface{}, error)
}

func NetworkGetInstanceResolve(p graphql.ResolveParams) (interface{}, error) {
	resolver := p.Source.(map[string]interface{})["Resolver"].(Resolver)
	astLoc := p.Info.FieldASTs[0].GetLoc()
	rawSubQuery := astLoc.Source.Body[astLoc.Start:astLoc.End]
	subQueryWithoutInstance, err := replaceInstanceName(p.Info.FieldName, rawSubQuery)
	if err != nil {
		return nil, fmt.Errorf("could not replace instance name in sub-query: %s", err)
	}

	params := network.ProxyGetInstanceParams{
		SubQuery:       network.ParseSubQuery(subQueryWithoutInstance),
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
