package network_introspect

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
)

func GenWeaviateNetworkIntrospectPropertiesObjField() *graphql.Field {
	weaviateNetworkIntrospectPropertiesObject := graphql.NewObject(
		graphql.ObjectConfig{
			Name: "WeaviateNetworkIntrospectPropertiesObj",
			Fields: graphql.Fields{
				"propertyName": &graphql.Field{
					Type:        graphql.String,
					Description: descriptions.WherePropertiesPropertyNameDesc,
				},
				"certainty": &graphql.Field{
					Type:        graphql.Float,
					Description: descriptions.WhereCertaintyDesc,
				},
			},
			Description: descriptions.WherePropertiesObjDesc,
		},
	)

	weaviateNetworkIntrospectPropertiesObjField := &graphql.Field{
		Name:        "WeaviateNetworkIntrospectPropertiesObj",
		Description: descriptions.WherePropertiesObjDesc,
		Type:        graphql.NewList(weaviateNetworkIntrospectPropertiesObject),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return weaviateNetworkIntrospectPropertiesObjField
}

func genNetworkIntrospectThingsAndActionsFilterFields(filterContainer *utils.FilterContainer) graphql.InputObjectConfigFieldMap {
	weaviateNetworkIntrospectWherePropertiesObj := genWeaviateNetworkIntrospectWherePropertiesObj(filterContainer)
	weaviateNetworkIntrospectWhereClassObj := genWeaviateNetworkIntrospectWhereClassObj(filterContainer)

	networkIntrospectThingsAndActionsFilterFields := graphql.InputObjectConfigFieldMap{
		"class": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(weaviateNetworkIntrospectWhereClassObj),
			Description: descriptions.WhereClassDesc,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(weaviateNetworkIntrospectWherePropertiesObj),
			Description: descriptions.WherePropertiesDesc,
		},
	}

	return networkIntrospectThingsAndActionsFilterFields
}

func genWeaviateNetworkIntrospectWherePropertiesObj(filterContainer *utils.FilterContainer) *graphql.InputObject {
	filterPropertiesElements := graphql.InputObjectConfigFieldMap{
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.FirstDesc,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: descriptions.WhereCertaintyDesc,
		},
		"name": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: descriptions.WhereNameDesc,
		},
		"keywords": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(filterContainer.WeaviateNetworkWhereKeywordsInpObj),
			Description: descriptions.WhereKeywordsDesc,
		},
	}

	weaviateNetworkIntrospectWherePropertiesObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        "WeaviateNetworkIntrospectWherePropertiesObj",
			Fields:      filterPropertiesElements,
			Description: descriptions.WherePropertiesObjDesc,
		},
	)

	return weaviateNetworkIntrospectWherePropertiesObj
}

func genWeaviateNetworkIntrospectWhereClassObj(filterContainer *utils.FilterContainer) *graphql.InputObject {
	filterClassElements := graphql.InputObjectConfigFieldMap{
		"name": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: descriptions.WhereNameDesc,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: descriptions.WhereCertaintyDesc,
		},
		"keywords": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(filterContainer.WeaviateNetworkWhereKeywordsInpObj),
			Description: descriptions.WhereKeywordsDesc,
		},
		"first": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.FirstDesc,
		},
	}

	weaviateNetworkIntrospectWhereClassObj := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name:        "WeaviateNetworkIntrospectWhereClassObj",
			Fields:      filterClassElements,
			Description: descriptions.WherePropertiesObjDesc,
		},
	)
	return weaviateNetworkIntrospectWhereClassObj
}
