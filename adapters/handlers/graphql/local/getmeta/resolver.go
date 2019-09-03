//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package getmeta provides the local get meta graphql endpoint for Weaviate
package getmeta

import (
	"context"
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the Meta feature must implement this interface.
type Resolver interface {
	LocalMeta(ctx context.Context, principal *models.Principal, info *traverser.MetaParams) (interface{}, error)
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Local.Meta queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}

func makeResolveClass(kind kind.Kind) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		className := schema.ClassName(p.Info.FieldName)
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected source to be a map, but was %t", p.Source)
		}

		resolver, ok := source["Resolver"].(Resolver)
		if !ok {
			return nil, fmt.Errorf("expected source to contain a usable Resolver, but was %t", p.Source)
		}

		cfg, ok := source["Config"].(config.Config)
		if !ok {
			return nil, fmt.Errorf("expected source to contain a config, but was %t", p.Source)
		}

		// There can only be exactly one ast.Field; it is the class name.
		if len(p.Info.FieldASTs) != 1 {
			panic("Only one Field expected here")
		}

		selections := p.Info.FieldASTs[0].SelectionSet
		properties, err := extractMetaProperties(selections)
		if err != nil {
			return nil, fmt.Errorf("could not extract properties for class '%s': %s", className, err)
		}

		filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract filters: %s", err)
		}

		analytics, err := common_filters.ExtractAnalyticsProps(p.Args, cfg.AnalyticsEngine)
		if err != nil {
			return nil, fmt.Errorf("could not extract analytics props: %s", err)
		}

		params := &traverser.MetaParams{
			Kind:       kind,
			Filters:    filters,
			ClassName:  className,
			Properties: properties,
			Analytics:  analytics,
		}

		// Log the request
		requestsLog := source["RequestsLog"].(RequestsLog)
		go func() {
			requestsLog.Register(telemetry.TypeGQL, telemetry.LocalQueryMeta)
		}()

		return resolver.LocalMeta(p.Context, principalFromContext(p.Context), params)
	}
}

func extractMetaProperties(selections *ast.SelectionSet) ([]traverser.MetaProperty, error) {
	var properties []traverser.MetaProperty

	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		if name == "__typename" {
			continue
		}

		property := traverser.MetaProperty{Name: schema.PropertyName(name)}
		analysesProps, err := extractPropertyAnalyses(field.SelectionSet)
		if err != nil {
			return nil, err
		}

		if len(analysesProps) == 0 {
			// this could be the case if the user only asked for __typename, but
			// nothing else, we then don't want to include this property and forwared
			// to the db connector.
			continue
		}

		property.StatisticalAnalyses = analysesProps
		properties = append(properties, property)
	}

	return properties, nil
}

func extractPropertyAnalyses(selections *ast.SelectionSet) ([]traverser.StatisticalAnalysis, error) {
	analyses := []traverser.StatisticalAnalysis{}
	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value

		if name == "__typename" {
			// skip, we want to let graphql serve this internatl meta field, not pass
			// this on to the resolve
			continue
		}

		property, err := traverser.ParseAnalysisProp(name)
		if err != nil {
			return nil, err
		}

		if property == traverser.TopOccurrences {
			// TopOccurrences is the only nested prop for now. It does have two
			// subprops which we predict to be computed in the same query with
			// neglible additional cost. In this case, we can save the effort of
			// actually parsing the subprops and just always return both subprops. If
			// we find this to be too slow (unlikely) and find out that the user
			// always only wants one of the two props (unlikely, as one is
			// meaningless without the other), then we can improve this and actually
			// parse the values.
			analyses = append(analyses, traverser.TopOccurrencesValue, traverser.TopOccurrencesOccurs)
			continue
		}

		analyses = append(analyses, property)
	}

	return analyses, nil
}

func principalFromContext(ctx context.Context) *models.Principal {
	principal := ctx.Value("principal")
	if principal == nil {
		return nil
	}

	return principal.(*models.Principal)
}
