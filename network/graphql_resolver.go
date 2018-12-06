package network

import (
	"fmt"
	"net/http"
)

type ProxyGetInstanceParams struct {
	SubQuery       []byte
	TargetInstance string
}

// ResolveGraphQLRequest resolves the Network part of a GQL request
func (n *network) ProxyGetInstance(params ProxyGetInstanceParams) (interface{}, error) {
	peer, err := n.GetPeerByName(params.TargetInstance)
	if err != nil {
		return nil, fmt.Errorf("could not connect to %s: %s", params.TargetInstance, err)
	}

	http.Post(fmt.Sprintf("%s/weaviate/v1/graphql", peer.URI), "", nil)
	return nil, nil
}
