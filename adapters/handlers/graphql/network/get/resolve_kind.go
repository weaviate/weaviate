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
package network_get

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/graphql-go/graphql"
)

func resolveKind(k kind.Kind) func(p graphql.ResolveParams) (interface{}, error) {
	kindField := kindNameInResult(k)

	return func(p graphql.ResolveParams) (interface{}, error) {
		firstLevel, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected source to be map[string]interface{}, was %#v", p.Source)
		}

		secondLevel, ok := firstLevel[kindField].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected source to be map[string]map[string][]interface{}, was %#v", firstLevel[kindField])
		}

		return secondLevel[p.Info.FieldName], nil
	}
}

// ResolveAction as part of a network query: Network -> Get -> Action -> <nameOfAction>
var ResolveAction = resolveKind(kind.Action)

// ResolveThing as part of a network query: Network -> Get -> Thing -> <nameOfThing>
var ResolveThing = resolveKind(kind.Thing)

func kindNameInResult(k kind.Kind) string {
	switch k {
	case kind.Thing:
		return "Things"
	case kind.Action:
		return "Actions"
	default:
		return ""
	}
}
