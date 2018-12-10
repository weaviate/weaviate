package p2p

import (
	"context"
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/client"
	"github.com/creativesoftwarefdn/weaviate/client/graphql"
	networkGet "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/get"
	"github.com/creativesoftwarefdn/weaviate/models"
)

// ProxyGetInstance proxies a single SubQuery to a single
// Target Instance. It is inteded to be called multiple
// times if you need to Network.Get from multiple instances.
func (n *network) ProxyGetInstance(params networkGet.ProxyGetInstanceParams) (*models.GraphQLResponse, error) {
	peer, err := n.GetPeerByName(params.TargetInstance)
	if err != nil {
		knownPeers, _ := n.ListPeers()
		return nil, fmt.Errorf("could not connect to %s: %s, known peers are %#v", params.TargetInstance, err, knownPeers)
	}

	peerClient, err := peer.CreateClient()
	if err != nil {
		return nil, fmt.Errorf("could not build client for peer %s: %s", peer.Name, err)
	}

	result, err := postToPeer(peerClient, params.SubQuery)
	if err != nil {
		return nil, fmt.Errorf("could post to peer %s: %s", peer.Name, err)
	}

	return result.Payload, nil
}

func postToPeer(client *client.WeaviateDecentralisedKnowledgeGraph, subQuery networkGet.SubQuery) (*graphql.WeaviateGraphqlPostOK, error) {
	localContext := context.Background()
	localContext, _ = context.WithTimeout(localContext, 1*time.Second)
	requestParams := &graphql.WeaviateGraphqlPostParams{
		Body:    &models.GraphQLQuery{Query: subQuery.WrapInLocalQuery()},
		Context: localContext,
	}
	return client.Graphql.WeaviateGraphqlPost(requestParams, nil)
}
