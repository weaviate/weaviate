package network

import (
	"context"
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/client"
	"github.com/creativesoftwarefdn/weaviate/client/graphql"
	"github.com/creativesoftwarefdn/weaviate/models"
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

	peerClient, err := peer.CreateClient()
	if err != nil {
		return nil, fmt.Errorf("could not build client for peer %s: %s", peer.Name, err)
	}

	_, err = postToPeer(peerClient, params.SubQuery)
	if err != nil {
		return nil, fmt.Errorf("could post to peer %s: %s", peer.Name, err)
	}

	return nil, nil
}

func postToPeer(client *client.WeaviateDecentralisedKnowledgeGraph, subQuery []byte) (interface{}, error) {
	localContext := context.Background()
	localContext, _ = context.WithTimeout(localContext, 1*time.Second)
	requestParams := &graphql.WeaviateGraphqlPostParams{
		Body:    &models.GraphQLQuery{Query: "foo"},
		Context: localContext,
	}
	return client.Graphql.WeaviateGraphqlPost(requestParams, nil)
}
