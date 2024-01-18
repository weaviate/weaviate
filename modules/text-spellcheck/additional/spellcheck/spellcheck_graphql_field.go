//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spellcheck

import (
	"fmt"

	"github.com/tailor-inc/graphql"
)

func (p *SpellCheckProvider) additionalSpellCheckField(classname string) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewList(p.additionalSpellCheckObj(classname)),
	}
}

func (p *SpellCheckProvider) additionalSpellCheckObj(classname string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%sAdditionalSpellCheck", classname),
		Fields: graphql.Fields{
			"originalText":        &graphql.Field{Type: graphql.String},
			"didYouMean":          &graphql.Field{Type: graphql.String},
			"location":            &graphql.Field{Type: graphql.String},
			"numberOfCorrections": &graphql.Field{Type: graphql.Int},
			"changes": &graphql.Field{
				Type: graphql.NewList(p.additionalSpellCheckChangesObj(classname)),
			},
		},
	})
}

func (p *SpellCheckProvider) additionalSpellCheckChangesObj(classname string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%sAdditionalSpellCheckChanges", classname),
		Fields: graphql.Fields{
			"original":  &graphql.Field{Type: graphql.String},
			"corrected": &graphql.Field{Type: graphql.String},
		},
	})
}
