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
package schema

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
)

type Cardinality int

const CardinalityAtMostOne Cardinality = 1
const CardinalityMany Cardinality = 2

func CardinalityOfProperty(property *models.SemanticSchemaClassProperty) Cardinality {
	if property.Cardinality == nil {
		return CardinalityAtMostOne
	}

	cardinality := *property.Cardinality
	switch cardinality {
	case "atMostOne":
		return CardinalityAtMostOne
	case "many":
		return CardinalityMany
	default:
		panic(fmt.Sprintf("Illegal cardinality %s", cardinality))
	}
}
