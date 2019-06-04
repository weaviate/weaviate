/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package fetch

import (
	"context"
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the GetMeta feature must implement this interface.
type Resolver interface {
	LocalFetchKindClass(ctx context.Context, principal *models.Principal, info *traverser.FetchSearch) (interface{}, error)
	LocalFetchFuzzy(ctx context.Context, principal *models.Principal, info traverser.FetchFuzzySearch) (interface{}, error)
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Local.Fetch queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}

func makeResolveClass(kind kind.Kind) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		resources, err := newResources(p.Source)
		if err != nil {
			return nil, err
		}

		params, err := parseWhere(p.Args, kind)
		if err != nil {
			return nil, fmt.Errorf("invalid where filter: %s", err)
		}

		go func() {
			resources.requestsLog.Register(telemetry.TypeGQL, telemetry.LocalQuery)
		}()

		return func() (interface{}, error) {
			return resources.resolver.LocalFetchKindClass(p.Context, principalFromContext(p.Context), params)
		}, nil

	}
}

type resources struct {
	resolver    Resolver
	requestsLog RequestsLog
}

func newResources(s interface{}) (*resources, error) {
	source, ok := s.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected source to be a map, but was %T", source)
	}

	resolver, ok := source["Resolver"].(Resolver)
	if !ok {
		return nil, fmt.Errorf("expected source to contain a usable Resolver, but was %#v", source)
	}

	requestsLog, ok := source["RequestsLog"].(RequestsLog)
	if !ok {
		return nil, fmt.Errorf("expected source to contain a usable RequestsLog, but was %#v", source)
	}

	return &resources{
		resolver:    resolver,
		requestsLog: requestsLog,
	}, nil
}

func resolveFuzzy(p graphql.ResolveParams) (interface{}, error) {
	resources, err := newResources(p.Source)
	if err != nil {
		return nil, err
	}

	params := extractFuzzyArgs(p)

	// words := resources.contextionary.SafeGetSimilarWordsWithCertainty(args.value, args.certainty)

	return resources.resolver.LocalFetchFuzzy(p.Context, principalFromContext(p.Context), params)
}

func extractFuzzyArgs(p graphql.ResolveParams) traverser.FetchFuzzySearch {
	var args traverser.FetchFuzzySearch

	// all args are required, so we don't need to check their existance
	args.Value = p.Args["value"].(string)
	args.Certainty = float32(p.Args["certainty"].(float64))

	return args
}

func principalFromContext(ctx context.Context) *models.Principal {
	principal := ctx.Value("principal")
	if principal == nil {
		return nil
	}

	return principal.(*models.Principal)
}
