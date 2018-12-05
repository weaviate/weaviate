package network_get

import "github.com/graphql-go/graphql"

type NetworkGetParams struct {
	SubQuery       []byte
	TargetInstance string
}

type Resolver interface {
	ProxyNetworkGet(info *NetworkGetParams) (func() interface{}, error)
}

func NetworkGetResolve(p graphql.ResolveParams) (interface{}, error) {
	resolver := p.Source.(map[string]interface{})["Resolver"].(Resolver)
	astLoc := p.Info.FieldASTs[0].GetLoc()
	params := &NetworkGetParams{
		SubQuery: astLoc.Source.Body[astLoc.Start:astLoc.End],
	}
	resolver.ProxyNetworkGet(params)

	return nil, nil
}
