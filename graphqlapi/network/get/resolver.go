package network_get

import (
	"fmt"
	"regexp"

	"github.com/graphql-go/graphql"
)

type NetworkGetInstanceParams struct {
	SubQuery       []byte
	TargetInstance string
}

type Resolver interface {
	ProxyNetworkGetInstance(info *NetworkGetInstanceParams) (func() interface{}, error)
}

func NetworkGetInstanceResolve(p graphql.ResolveParams) (interface{}, error) {
	resolver := p.Source.(map[string]interface{})["Resolver"].(Resolver)
	astLoc := p.Info.FieldASTs[0].GetLoc()
	rawSubQuery := astLoc.Source.Body[astLoc.Start:astLoc.End]
	subQueryWithoutInstance, err := replaceInstanceName(p.Info.FieldName, rawSubQuery)
	if err != nil {
		return nil, fmt.Errorf("could not replace instance name in sub-query: %s", err)
	}

	params := &NetworkGetInstanceParams{
		SubQuery:       subQueryWithoutInstance,
		TargetInstance: p.Info.FieldName,
	}
	resolver.ProxyNetworkGetInstance(params)

	return nil, nil
}

func replaceInstanceName(instanceName string, query []byte) ([]byte, error) {
	r, err := regexp.Compile(fmt.Sprintf(`^%s\s*`, instanceName))
	if err != nil {
		return []byte{}, err
	}

	return r.ReplaceAll(query, []byte("Get ")), nil

}
