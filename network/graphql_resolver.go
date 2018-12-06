package network

import (
	"context"
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/client"
	"github.com/creativesoftwarefdn/weaviate/client/graphql"
	"github.com/creativesoftwarefdn/weaviate/models"
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

// ProxyGetInstance proxies a single SubQuery to a single
// Target Instance. It is inteded to be called multiple
// times if you need to Network.Get from multiple instances.
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

func postToPeer(client *client.WeaviateDecentralisedKnowledgeGraph, subQuery SubQuery) (interface{}, error) {
	localContext := context.Background()
	localContext, _ = context.WithTimeout(localContext, 1*time.Second)
	requestParams := &graphql.WeaviateGraphqlPostParams{
		Body:    &models.GraphQLQuery{Query: subQuery.WrapInLocalQuery()},
		Context: localContext,
	}
	return client.Graphql.WeaviateGraphqlPost(requestParams, nil)
}
