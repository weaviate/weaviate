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

package explore

import (
	"context"
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the GetMeta feature must implement this interface.
type Resolver interface {
	Explore(ctx context.Context, principal *models.Principal,
		params traverser.ExploreParams) ([]traverser.VectorSearchResult, error)
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Local.Fetch queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
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

func resolve(p graphql.ResolveParams) (interface{}, error) {
	resources, err := newResources(p.Source)
	if err != nil {
		return nil, err
	}

	params := extractFuzzyArgs(p)

	return resources.resolver.Explore(p.Context,
		principalFromContext(p.Context), params)
}

func extractFuzzyArgs(p graphql.ResolveParams) traverser.ExploreParams {
	var args traverser.ExploreParams

	// keywords is a required argument, so we don't need to check for its existing
	keywords := p.Args["concepts"].([]interface{})
	args.Values = make([]string, len(keywords), len(keywords))
	for i, value := range keywords {
		args.Values[i] = value.(string)
	}

	// limit is an optional arg, so it could be nil
	limit, ok := p.Args["limit"]
	if ok {
		// the type is fixed through gql config, no need to catch incorrect type
		// assumption
		args.Limit = limit.(int)
	}

	// moveTo is an optional arg, so it could be nil
	moveTo, ok := p.Args["moveTo"]
	if ok {
		args.MoveTo = extractMovement(moveTo)
	}

	// moveAwayFrom is an optional arg, so it could be nil
	moveAwayFrom, ok := p.Args["moveAwayFrom"]
	if ok {
		args.MoveAwayFrom = extractMovement(moveAwayFrom)
	}

	return args
}

func extractMovement(input interface{}) traverser.ExploreMove {
	// the type is fixed through gql config, no need to catch incorrect type
	// assumption, all fields are required so we don't need to check for their
	// presence
	moveToMap := input.(map[string]interface{})
	res := traverser.ExploreMove{}
	res.Force = float32(moveToMap["force"].(float64))

	keywords := moveToMap["concepts"].([]interface{})
	res.Values = make([]string, len(keywords), len(keywords))
	for i, value := range keywords {
		res.Values[i] = value.(string)
	}

	return res
}

func principalFromContext(ctx context.Context) *models.Principal {
	principal := ctx.Value("principal")
	if principal == nil {
		return nil
	}

	return principal.(*models.Principal)
}
