package schema

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/models"
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
