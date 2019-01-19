package getmeta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

func textPropertyFields(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaPointingFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        fmt.Sprintf("Meta%sType", class.Class),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("Meta%sCount", class.Class),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("Meta%sTopOccurrences", class.Class),
			Description: descriptions.GetMetaPropertyTopOccurrencesDesc,
			Type:        graphql.NewList(textTopOccurrences(class, property)),
			Args: graphql.FieldConfigArgument{
				"first": &graphql.ArgumentConfig{
					Description: descriptions.FirstDesc,
					Type:        graphql.Int,
				},
				"after": &graphql.ArgumentConfig{
					Description: descriptions.AfterDesc,
					Type:        graphql.Int,
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getMetaTextProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("Meta%s%sObj", class.Class, property.Name),
		Fields:      getMetaPointingFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(getMetaTextProperty)
}

func textTopOccurrences(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaPointingFields := graphql.Fields{
		"value": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sTopOccurrencesValue", class.Class, property.Name),
			Description: descriptions.GetMetaPropertyTopOccurrencesValueDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("Meta%s%sTopOccurrencesOccurs", class.Class, property.Name),
			Description: descriptions.GetMetaPropertyTopOccurrencesOccursDesc,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getMetaPointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("Meta%s%sTopOccurrencesObj", class.Class, property.Name),
		Fields:      getMetaPointingFields,
		Description: descriptions.GetMetaPropertyTopOccurrencesDesc,
	}

	return graphql.NewObject(getMetaPointing)
}
