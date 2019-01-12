package get_meta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

func refPropertyObj(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	fields := graphql.Fields{
		"type": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sType", class.Class, property.Name),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sCount", class.Class, property.Name),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"pointingTo": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sPointingTo", class.Class, property.Name),
			Description: descriptions.GetMetaClassPropertyPointingToDesc,
			Type:        graphql.NewList(graphql.String),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Meta%s%sObj", class.Class, property.Name),
		Fields:      fields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	})
}
