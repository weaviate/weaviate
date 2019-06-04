package explore

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func conceptsFieldsObj() *graphql.Object {
	getLocalExploreConceptsFields := graphql.Fields{
		"className": &graphql.Field{
			Name: "WeaviateLocalExploreConceptsClassName",
			// Description: descriptions.LocalExploreConceptsClassName,
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(kinds.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore.Concepts.className resolver", p.Source)
				}

				return vsr.ClassName, nil
			},
		},

		"beacon": &graphql.Field{
			Name: "WeaviateLocalExploreConceptsBeacon",
			// Description: descriptions.LocalExploreConceptsBeacon,
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				vsr, ok := p.Source.(kinds.VectorSearchResult)
				if !ok {
					return nil, fmt.Errorf("unknown type %T in Explore.Concepts.className resolver", p.Source)
				}

				return vsr.Beacon, nil
			},
		},
	}

	getLocalExploreConceptsFieldsObject := graphql.ObjectConfig{
		Name:   "WeaviateLocalExploreConceptsObj",
		Fields: getLocalExploreConceptsFields,
		// Description: descriptions.LocalExploreConceptsObj,
	}

	return graphql.NewObject(getLocalExploreConceptsFieldsObject)
}
