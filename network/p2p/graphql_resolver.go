/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package p2p

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/creativesoftwarefdn/weaviate/client"
	"github.com/creativesoftwarefdn/weaviate/client/graphql"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network/common"
	"github.com/creativesoftwarefdn/weaviate/models"
)

// ProxyGetInstance proxies a single SubQuery to a single Target Instance. It
// is inteded to be called multiple times if you need to Network.Get from
// multiple instances.
func (n *network) ProxyGetInstance(params common.Params) (*models.GraphQLResponse, error) {
	return n.proxy(params)
}

// ProxyGetMetaInstance proxies a single SubQuery to a single Target Instance. It
// is inteded to be called multiple times if you need to Network.GetMeta from
// multiple instances.
func (n *network) ProxyGetMetaInstance(params common.Params) (*models.GraphQLResponse, error) {
	return n.proxy(params)
}

// ProxyAggregateInstance proxies a single SubQuery to a single Target Instance. It
// is inteded to be called multiple times if you need to Network.Aggregate from
// multiple instances.
func (n *network) ProxyAggregateInstance(params common.Params) (*models.GraphQLResponse, error) {
	return n.proxy(params)
}

func (n *network) proxy(params common.Params) (*models.GraphQLResponse, error) {
	peer, err := n.GetPeerByName(params.TargetInstance)
	if err != nil {
		knownPeers, _ := n.ListPeers()
		return nil, fmt.Errorf("could not connect to %s: %s, known peers are %#v",
			params.TargetInstance, err, knownPeers)
	}

	peerClient, err := peer.CreateClient()
	if err != nil {
		return nil, fmt.Errorf("could not build client for peer %s: %s", peer.Name, err)
	}

	result, err := postToPeer(peerClient, params.SubQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("could not post to peer %s: %s", peer.Name, err)
	}

	return result.Payload, nil
}

func postToPeer(client *client.WeaviateDecentralisedKnowledgeGraph, subQuery common.SubQuery,
	principal interface{}) (*graphql.WeaviateGraphqlPostOK, error) {
	localContext := context.Background()
	localContext, cancel := context.WithTimeout(localContext, 1*time.Second)
	defer cancel()
	requestParams := &graphql.WeaviateGraphqlPostParams{
		Body:    &models.GraphQLQuery{Query: subQuery.WrapInLocalQuery()},
		Context: localContext,
		// re-enable once we have auth again
		// HTTPClient: clientWithTokenInjectorRoundTripper(principal),
	}
	return client.Graphql.WeaviateGraphqlPost(requestParams, nil)
}

type tokenInjectorRoundTripper struct {
	key   string
	token string
}

func (rt *tokenInjectorRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-API-KEY", rt.key)
	req.Header.Set("X-API-TOKEN", rt.token)
	return http.DefaultTransport.RoundTrip(req)
}

func clientWithTokenInjectorRoundTripper(principal interface{}) *http.Client {
	return &http.Client{
		Transport: &tokenInjectorRoundTripper{
			// key:   string(principal.KeyID),
			// token: string(principal.Token),
		},
	}
}
