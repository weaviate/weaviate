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
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/annotate"
)

const PropertyAnnotate = annotate.Property

// AnnotateProvider serves the annotate additional property for a decisions
// module. The property is available through gRPC only: it has no GraphQL
// field.
type AnnotateProvider struct {
	provider *annotate.Provider
}

func NewAnnotateProvider(client annotate.Client) *AnnotateProvider {
	return &AnnotateProvider{provider: annotate.New(client)}
}

func (p *AnnotateProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return map[string]modulecapabilities.AdditionalProperty{
		PropertyAnnotate: {
			DefaultValue: p.provider.AdditionalPropertyDefaultValue(),
			SearchFunctions: modulecapabilities.AdditionalSearch{
				ExploreGet:  p.provider.AdditionalPropertyFn,
				ExploreList: p.provider.AdditionalPropertyFn,
			},
		},
	}
}
