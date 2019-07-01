package explore

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// Build builds the object containing the Local->Explore Fields, such as Things/Actions
func Build() *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateLocalExplore",
		Description: descriptions.LocalExplore,
		Type:        graphql.NewList(exploreObject()),
		Resolve:     resolve,
		Args: graphql.FieldConfigArgument{
			"concepts": &graphql.ArgumentConfig{
				Description: descriptions.Keywords,
				Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
			},
			"limit": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: descriptions.Limit,
			},
			"moveTo": &graphql.ArgumentConfig{
				Description: descriptions.VectorMovement,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:   "WeaviateLocalExploreMoveTo",
						Fields: movementInp(),
					}),
			},
			"moveAwayFrom": &graphql.ArgumentConfig{
				Description: descriptions.VectorMovement,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:   "WeaviateLocalExploreMoveAwayFrom",
						Fields: movementInp(),
					}),
			},
		},
	}
}

func exploreObject() *graphql.Object {
	getLocalExploreFields := graphql.Fields{
		"className": &graphql.Field{
			Name:        "WeaviateLocalExploreClassName",
			Description: descriptions.ClassName,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.ClassName, nil
			},
		},

		"beacon": &graphql.Field{
			Name:        "WeaviateLocalExploreBeacon",
			Description: descriptions.Beacon,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.Beacon, nil
			},
		},

		"distance": &graphql.Field{
			Name:        "WeaviateLocalExploreBeacon",
			Description: descriptions.Distance,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(traverser.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore..className resolver", p.Source)
				}

				return vsr.Distance, nil
			},
		},
	}

	getLocalExploreFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalExploreObj",
		Fields:      getLocalExploreFields,
		Description: descriptions.LocalExplore,
	}

	return graphql.NewObject(getLocalExploreFieldsObject)
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
