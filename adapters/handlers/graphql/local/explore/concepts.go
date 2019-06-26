package explore

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func conceptsField() *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateLocalExploreConcepts",
		Description: descriptions.LocalExploreConcepts,
		Type:        graphql.NewList(conceptsFieldsObj()),
		Args: graphql.FieldConfigArgument{
			"keywords": &graphql.ArgumentConfig{
				Description: descriptions.Keywords,
				Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
			},
			"limit": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: descriptions.Limit,
			},
			"moveTo": &graphql.ArgumentConfig{
				Description: descriptions.LocalExploreConceptsMovement,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:   "WeaviateLocalExploreMoveTo",
						Fields: movementInp(),
					}),
			},
			"moveAwayFrom": &graphql.ArgumentConfig{
				Description: descriptions.LocalExploreConceptsMovement,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:   "WeaviateLocalExploreMoveAwayFrom",
						Fields: movementInp(),
					}),
			},
		},
		Resolve: resolveConcepts,
	}
}

func conceptsFieldsObj() *graphql.Object {
	getLocalExploreConceptsFields := graphql.Fields{
		"className": &graphql.Field{
			Name:        "WeaviateLocalExploreConceptsClassName",
			Description: descriptions.ClassName,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore.Concepts.className resolver", p.Source)
				}

				return vsr.ClassName, nil
			},
		},

		"beacon": &graphql.Field{
			Name:        "WeaviateLocalExploreConceptsBeacon",
			Description: descriptions.Beacon,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore.Concepts.className resolver", p.Source)
				}

				return vsr.Beacon, nil
			},
		},

		"distance": &graphql.Field{
			Name:        "WeaviateLocalExploreConceptsBeacon",
			Description: descriptions.Distance,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore.Concepts.className resolver", p.Source)
				}

				return vsr.Distance, nil
			},
		},
	}

	getLocalExploreConceptsFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalExploreConceptsObj",
		Fields:      getLocalExploreConceptsFields,
		Description: descriptions.LocalExploreConcepts,
	}

	return graphql.NewObject(getLocalExploreConceptsFieldsObject)
}

func movementInp() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"keywords": &graphql.InputObjectFieldConfig{
			Description: descriptions.Keywords,
			Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"force": &graphql.InputObjectFieldConfig{
			Description: descriptions.Force,
			Type:        graphql.NewNonNull(graphql.Float),
		},
	}
}
