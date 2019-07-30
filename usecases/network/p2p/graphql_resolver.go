//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package p2p

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/network/common"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/network/fetch"
	"github.com/semi-technologies/weaviate/client"
	"github.com/semi-technologies/weaviate/client/graphql"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
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

	return n.sendQueryToPeer(params.SubQuery, peer)
}

type peerResponse struct {
	res  *models.GraphQLResponse
	err  error
	name string
}

func (n *network) ProxyFetch(q common.SubQuery) ([]fetch.Response, error) {
	var results []fetch.Response
	knownPeers, err := n.ListPeers()
	if err != nil {
		return nil, err
	}

	wg := &sync.WaitGroup{}
	resultsC := make(chan peerResponse, len(knownPeers))
	for _, peer := range knownPeers {
		wg.Add(1)
		go func(peer peers.Peer) {
			defer wg.Done()
			res, err := n.sendQueryToPeer(q, peer)
			resultsC <- peerResponse{res, err, peer.Name}
		}(peer)
	}

	wg.Wait()
	close(resultsC)
	for res := range resultsC {
		if res.err != nil {
			return nil, res.err
		}

		results = append(results, fetch.Response{
			GraphQL:  res.res,
			PeerName: res.name,
		})
	}

	return results, nil
}

func (n *network) sendQueryToPeer(q common.SubQuery, peer peers.Peer) (*models.GraphQLResponse, error) {
	peerClient, err := peer.CreateClient()
	if err != nil {
		return nil, fmt.Errorf("could not build client for peer %s: %s", peer.Name, err)
	}

	result, err := postToPeer(peerClient, q, nil)
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
		Body:    &models.GraphQLQuery{Query: subQuery.String()},
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
