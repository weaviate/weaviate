package get

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func exploreArgument(kindName, className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("WeaviateLocalGet%ss%s", kindName, className)
	return &graphql.ArgumentConfig{
		// Description: descriptions.LocalGetExplore,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sExploreInpObj", prefix),
				Fields:      exploreFields(prefix),
				Description: descriptions.LocalGetWhereInpObj,
			},
		),
	}
}

func exploreFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"concepts": &graphql.InputObjectFieldConfig{
			// Description: descriptions.Concepts,
			Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"moveTo": &graphql.InputObjectFieldConfig{
			Description: descriptions.LocalExploreConceptsMovement,
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:   fmt.Sprintf("%sMoveTo", prefix),
					Fields: movementInp(),
				}),
		},
		"moveAwayFrom": &graphql.InputObjectFieldConfig{
			Description: descriptions.LocalExploreConceptsMovement,
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:   fmt.Sprintf("%sMoveAwayFrom", prefix),
					Fields: movementInp(),
				}),
		},
	}
}

func movementInp() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"concepts": &graphql.InputObjectFieldConfig{
			Description: descriptions.Keywords,
			Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"force": &graphql.InputObjectFieldConfig{
			Description: descriptions.Force,
			Type:        graphql.NewNonNull(graphql.Float),
		},
	}
}
