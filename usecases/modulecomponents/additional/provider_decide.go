//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package additional

import (
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/decide"
)

const PropertyDecide = decide.Property

// DecideProvider serves the decide additional property for a decisions
// module. The property is available through gRPC only: it has no GraphQL
// field.
type DecideProvider struct {
	provider *decide.Provider
}

func NewDecideProvider(client decide.Client) *DecideProvider {
	return &DecideProvider{provider: decide.New(client)}
}

func (p *DecideProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return map[string]modulecapabilities.AdditionalProperty{
		PropertyDecide: {
			DefaultValue: p.provider.AdditionalPropertyDefaultValue(),
			SearchFunctions: modulecapabilities.AdditionalSearch{
				ExploreGet:  p.provider.AdditionalPropertyFn,
				ExploreList: p.provider.AdditionalPropertyFn,
			},
		},
	}
}
