/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package graphqlapi

import (
	"fmt"
	//"github.com/creativesoftwarefdn/weaviate/models"
	//"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/graphql-go/graphql"
)

// Build the GraphQL schema based on
// 1) the static query structure (e.g. LocalFetch)
// 2) the (dynamic) database schema from Weaviate

func (g *GraphQL) buildGraphqlSchema() error {
	
	var err error
	var rootFields graphql.Fields
	
	rootFields, err = g.genRootQueryFields()
	
	if err != nil {
		return fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	}
	
	rootQuery := graphql.ObjectConfig{
		Name: "WeaviateObj",
		Fields: rootFields,
		Description: "Location of the root query",
	}
	
	g.weaviateGraphQLSchema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(rootQuery),
	})

	if err != nil {
		return fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	} else {
		return nil
	}
}

// this builds the root query
// dynamic allocation will happen when needed


func (g *GraphQL) genRootQueryFields() (graphql.Fields, error) {
	localField, err := g.buildLocalField()

	if err != nil {
		return nil, fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	}

	var rootFields = graphql.Fields{
		"Local": localField,
		"Network" : nil,
	}
	
	return rootFields, nil
}

func (g *GraphQL) buildLocalField() (*graphql.Field, error) {
	// sample code for dynamic generation of action and thing fields
	//action_class_fields, err := g.buildExampleActionClassFields()
//	if err != nil {
//		return nil, err
//	}

	// TODO refactor: rename these variables(?) and move these function calls to a central, high level function
	localConvertedFetchObject := g.genThingsAndActionsFieldsForWeaviateLocalConvertedFetchObj()
	
	localMetaFetchObject := g.genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj()
	
	localMetaGenericsObject := g.genGenericsFieldForWeaviateLocalMetaFetchObj(localMetaFetchObject)
	
	localFields := graphql.Fields{
		
		"ConvertedFetch": &graphql.Field{
			Name: "WeaviateLocalConvertedFetch",
			Type: graphql.NewObject(localConvertedFetchObject),
			Description: "Do a converted fetch to search Things or Actions on the local weaviate",
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"MetaFetch": &graphql.Field{
			Name: "WeaviateLocalMetaFetch",
			Type: graphql.NewObject(localMetaGenericsObject),
			Description: "Fetch meta information about Things or Actions on the local weaviate",
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		// stub for implementing HelpersFetch. Commented out to avoid confusion. 
//		"HelpersFetch": &graphql.Field{
//			Name: "WeaviateLocalHelpersFetch",
//			Type: graphql.String, // TODO: make HelpersFetch have actual content
//			Description: "Do a helpers fetch to search Things or Actions on the local weaviate",
//			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
//				return nil, fmt.Errorf("Not supported")
//			},
//		},
//		//sample code to dynamically generate schema; should be one level lower
//		"ActionClasses": &graphql.Field{
//			Type: graphql.NewObject(graphql.ObjectConfig{Name: "SampleActionClass", Fields: action_class_fields}),
//			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
//				return nil, fmt.Errorf("Not supported")
//			},
//		},
	}

	weaviateLocalObject := graphql.ObjectConfig{
		Name: "WeaviateLocalObj", 
		Fields: localFields,
		Description: "Type of fetch on the internal Weaviate",
	}
	
	field := graphql.Field{
		Type: graphql.NewObject(weaviateLocalObject), 
		Description: "Locate on the local Weaviate", 
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}

	return &field, nil
}

func (g *GraphQL) genThingsAndActionsFieldsForWeaviateLocalConvertedFetchObj() graphql.ObjectConfig {
	
	fields:= graphql.Fields{
		"Actions": &graphql.Field{
			Name: "WeaviateLocalConvertedFetchActions",
			Description: "Locate Actions on the local Weaviate",
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"Things": &graphql.Field{
			Name: "WeaviateLocalConvertedFetchThings",
			Description: "Locate Things on the local Weaviate",
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	fieldsObject := graphql.ObjectConfig{
		Name: "WeaviateLocalConvertedFetchObj", 
		Fields: fields,
		Description: "Fetch things or actions on the internal Weaviate",
	}
	return fieldsObject
}

func (g *GraphQL) genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj() graphql.ObjectConfig {
	
	fields:= graphql.Fields{
		"Actions": &graphql.Field{
			Name: "WeaviateLocalMetaFetchGenericsActions",
			Description: "Action to fetch for meta generic fetch",
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"Things": &graphql.Field{
			Name: "WeaviateLocalMetaFetchGenericsThings",
			Description: "Thing to fetch for meta generic fetch",
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	
	fieldsObject := graphql.ObjectConfig{
		Name: "WeaviateLocalMetaFetchGenericsObj", 
		Fields: fields,
		Description: "Object type to fetch",
	}
	
	return fieldsObject
}

func (g *GraphQL) genGenericsFieldForWeaviateLocalMetaFetchObj(localMetaFetchObject graphql.ObjectConfig) graphql.ObjectConfig {
	
	fields:= graphql.Fields{
		"Generics": &graphql.Field{
			Name: "WeaviateLocalMetaFetchGenericsObj",
			Description: "Fetch generic meta information based on the type",
			Type: graphql.NewObject(localMetaFetchObject),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	
	fieldsObject := graphql.ObjectConfig{
		Name: "WeaviateLocalMetaFetchObj", 
		Fields: fields,
		Description: "Fetch things or actions on the internal Weaviate",
	}
	
	return fieldsObject
}
// EXAMPLE: How to iterate through the
//func (g *GraphQL) buildExampleActionClassFields() (graphql.Fields, error) {
//	fields := graphql.Fields{}
//
//	for _, class := range g.databaseSchema.ActionSchema.Schema.Classes {
//		field, err := buildExampleActionClassField(class)
//		if err != nil {
//			return nil, err
//		} else {
//			fields[class.Class] = field
//		}
//	}
//
//	return fields, nil
//}
//
//func buildExampleActionClassField(class *models.SemanticSchemaClass) (*graphql.Field, error) {
//	return &graphql.Field{
//		Type: graphql.String,
//		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
//			return nil, fmt.Errorf("Not supported")
//		},
//	}, nil
//}

// you were digging through the structure of the dynamically interpreted schema yourself here
//g.databaseSchema.ActionSchema.Schema.Classes[0].Class  