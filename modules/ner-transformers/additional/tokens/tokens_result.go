//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package tokens

import (
	"context"
	"fmt"
	// "errors"
	// "strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	nermodels "github.com/semi-technologies/weaviate/modules/ner-transformers/additional/models"
)


func (p *TokenProvider) findTokens(ctx context.Context,
	in []search.Result, params *Params) ([]search.Result, error) {
		if len(in) > 0 {

			if len(in) == 0 {
				return nil, nil
			}

			if params == nil {
				return nil, fmt.Errorf("no params provided")
			}

			for i := range in { // for each result of the general GraphQL Query
				ap := in[i].AdditionalProperties
				if ap == nil {
					ap = models.AdditionalProperties{}
				}

			// 	// check params. for each property we need to do ner
			// 	for j, prop := range *params.properties {
					

			// 		// call ner module with that prop value as text
			// 		ap["tokens"] = &nermodels.Token{
			// 			// Property:       "TEXT_PROPERTY",
			// 			// Entity:         "TEST_ENTITY",
			// 			// Certainty:      0.94,
			// 			Word:           &word,
			// 			// StartPosition:  1,
			// 			// EndPosition:    2,
			// 		}
			// 	}
			// }

			// for i := range in {
				spew.Dump(params)
				certainty := params.Certainty
				ap["tokens"] = &nermodels.Token{
					// Property:       "TEXT_PROPERTY",
					// Entity:         "TEST_ENTITY",
					Certainty:         certainty,
					// Word:           &word,
					// StartPosition:  1,
					// EndPosition:    2,
				}
				in[i].AdditionalProperties = ap
			}
		}
		return in, nil
	}
