//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package getmeta

import (
	"fmt"
	"regexp"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/network/common"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
)

// Params ties a SubQuery and a single instance
// together
type Params struct {
	SubQuery       common.SubQuery
	TargetInstance string
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Network.Get queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}

// Resolver describes the requirements of this package
type Resolver interface {
	ProxyMetaInstance(info common.Params) (*models.GraphQLResponse, error)
}

func Resolve(p graphql.ResolveParams) (interface{}, error) {
	source, ok := p.Source.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected source to be a map[string]interface{}, but was \n%#v",
			p.Source)
	}

	resolver, ok := source["NetworkResolver"].(Resolver)
	if !ok {
		return nil, fmt.Errorf("expected source['NetworkResolver'] to be a Resolver, but was \n%#v",
			source["NetworkResolver"])
	}

	astLoc := p.Info.FieldASTs[0].GetLoc()
	rawSubQuery := astLoc.Source.Body[astLoc.Start:astLoc.End]
	subQueryWithoutInstance, err := replaceInstanceName(p.Info.FieldName, rawSubQuery)
	if err != nil {
		return nil, fmt.Errorf("could not replace instance name in sub-query: %s", err)
	}

	params := common.Params{
		SubQuery: common.ParseSubQuery(subQueryWithoutInstance).
			WrapInLocalQuery().
			WrapInBraces(),
		TargetInstance: p.Info.FieldName,
	}

	graphQLResponse, err := resolver.ProxyMetaInstance(params)
	if err != nil {
		return nil, fmt.Errorf("could not proxy to remote instance: %s", err)
	}

	local, ok := graphQLResponse.Data["Local"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected response.data.Local to be map[string]interface{}, but response was %#v",
			graphQLResponse.Data["Local"])
	}

	// Log the request
	requestsLog, ok := source["RequestsLog"].(RequestsLog)
	if !ok {
		return nil, fmt.Errorf("expected source['RequestsLog'] to be a RequestsLog, but was \n%#v",
			source["RequestsLog"])
	}
	go func() {
		requestsLog.Register(telemetry.TypeGQL, telemetry.NetworkQueryMeta)
	}()

	return local["Meta"], nil
}

func replaceInstanceName(instanceName string, query []byte) ([]byte, error) {
	r, err := regexp.Compile(fmt.Sprintf(`^%s\s*`, instanceName))
	if err != nil {
		return []byte{}, err
	}

	return r.ReplaceAll(query, []byte("Meta ")), nil
}
