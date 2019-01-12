package getmeta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

func numberPropertyFields(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaNumberFields := graphql.Fields{
		"sum": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sSum", class.Class, property.Name),
			Description: descriptions.GetMetaPropertySumDesc,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"type": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sType", class.Class, property.Name),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"lowest": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sLowest", class.Class, property.Name),
			Description: descriptions.GetMetaPropertyLowestDesc,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"highest": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sHighest", class.Class, property.Name),
			Description: descriptions.GetMetaPropertyHighestDesc,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"average": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sAverage", class.Class, property.Name),
			Description: descriptions.GetMetaPropertyAverageDesc,
			Type:        graphql.Float,
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
	}

	getMetaNumberProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("Meta%s%sObj", class.Class, property.Name),
		Fields:      getMetaNumberFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(getMetaNumberProperty)
}
