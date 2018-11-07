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

// Package graphqlapi provides the graphql endpoint for Weaviate
package graphqlapi

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

const propertyType string = "Datatype of the property"
const propertyCount string = "Total amount of found instances"
const propertyTopOccurrences string = "Object for the most frequent property values"
const propertyTopOccurrencesValue string = "The most frequently occurring value of this property in the dataset"
const propertyTopOccurrencesOccurs string = "Number of occurrence of this property value"
const propertyLowest string = "Lowest value found in the dataset for this property"
const propertyHighest string = "Highest value found in the dataset for this property"
const propertyAverage string = "Average value found in the dataset for this property"
const propertySum string = "Sum of values found in the dataset for this property"
const propertyObject string = "Object for property meta information"

// Build the dynamically generated GetMeta Things part of the schema
func genMetaClassFieldsFromSchema(databaseSchema []*models.SemanticSchemaClass, classParentTypeIsAction bool) (*graphql.Object, error) {
	classFields := graphql.Fields{}
	name := "WeaviateLocalGetMetaThingsObj"
	description := "Type of Things i.e. Things classes to GetMeta information of on the Local Weaviate"
	if classParentTypeIsAction {
		name = "WeaviateLocalGetMetaActionsObj"
		description = "Type of Actions i.e. Actions classes to GetMeta information of on the Local Weaviate"
	}

	for _, class := range databaseSchema {
		field, err := genMetaSingleClassField(class, class.Description)

		if err != nil {
			return nil, err
		}

		classFields[class.Class] = field
	}

	localGetMetaClasses := graphql.ObjectConfig{
		Name:        name,
		Fields:      classFields,
		Description: description,
	}

	return graphql.NewObject(localGetMetaClasses), nil
}

func genMetaSingleClassField(class *models.SemanticSchemaClass, description string) (*graphql.Field, error) {
	metaClassName := fmt.Sprintf("%s%s", "Meta", class.Class)

	singleClassPropertyFields := graphql.ObjectConfig{
		Name: metaClassName,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleClassPropertyFields, err := genMetaSingleClassPropertyFields(class)

			if err != nil {
				panic("Failed to assemble single Meta Class field")
			}

			return singleClassPropertyFields
		}),
		Description: description,
	}

	singleClassPropertyFieldsObject := graphql.NewObject(singleClassPropertyFields)
	singleClassPropertyFieldsField := &graphql.Field{
		Type:        singleClassPropertyFieldsObject,
		Description: class.Description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: "Pagination option, show the first x results",
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: "Pagination option, show the results after the first x results",
				Type:        graphql.Int,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return singleClassPropertyFieldsField, nil
}

func genMetaSingleClassPropertyFields(class *models.SemanticSchemaClass) (graphql.Fields, error) {
	singleClassPropertyFields := graphql.Fields{}
	metaPropertyObj := genMetaPropertyObj(class)

	metaPropertyObjField := &graphql.Field{
		Description: "Meta information about a class object and its (filtered) objects",
		Type:        metaPropertyObj,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	singleClassPropertyFields["meta"] = metaPropertyObjField

	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)

		if err != nil {
			return nil, err
		}

		convertedDataType, err := handleGetMetaNonObjectDataTypes(*propertyType, class, property)

		if err != nil {
			return nil, err
		}

		singleClassPropertyFields[property.Name] = convertedDataType
	}

	return singleClassPropertyFields, nil
}

func handleGetMetaNonObjectDataTypes(dataType schema.DataType, class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {
	metaClassStringPropertyFields := genMetaClassStringPropertyFields(class, property)
	metaClassIntPropertyFields := genMetaClassIntPropertyFields(class, property)
	metaClassNumberPropertyFields := genMetaClassNumberPropertyFields(class, property)
	metaClassBooleanPropertyFields := genMetaClassBooleanPropertyFields(class, property)
	metaClassDatePropertyFields := genMetaClassDatePropertyFields(class, property)
	metaClassCRefPropertyFields := genMetaClassCRefPropertyObj(class, property)

	switch dataType {

	case schema.DataTypeString:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, "Meta information about the property ", property.Name),
			Type:        metaClassStringPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}, nil

	case schema.DataTypeInt:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, "Meta information about the property ", property.Name),
			Type:        metaClassIntPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}, nil

	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, "Meta information about the property ", property.Name),
			Type:        metaClassNumberPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}, nil

	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, "Meta information about the property ", property.Name),
			Type:        metaClassBooleanPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}, nil

	case schema.DataTypeDate:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, "Meta information about the property ", property.Name),
			Type:        metaClassDatePropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}, nil

	case schema.DataTypeCRef:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, "Meta information about the property ", property.Name),
			Type:        metaClassCRefPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}, nil

	default:
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype)
	}
}

func genMetaClassStringPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	topOccurrencesFields := genMetaClassStringPropertyTopOccurrencesFields(class, property)

	getMetaPointingFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s", "Meta", class.Class, "Type"),
			Description: propertyType,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s", "Meta", class.Class, "Count"),
			Description: propertyCount,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s", "Meta", class.Class, "TopOccurrences"),
			Description: propertyTopOccurrences,
			Type:        graphql.NewList(topOccurrencesFields),
			Args: graphql.FieldConfigArgument{
				"first": &graphql.ArgumentConfig{
					Description: "Pagination option, show the first x results",
					Type:        graphql.Int,
				},
				"after": &graphql.ArgumentConfig{
					Description: "Pagination option, show the results after the first x results",
					Type:        graphql.Int,
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getMetaStringProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaPointingFields,
		Description: propertyObject,
	}

	return graphql.NewObject(getMetaStringProperty)
}

func genMetaClassStringPropertyTopOccurrencesFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaPointingFields := graphql.Fields{

		"value": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "TopOccurrencesValue"),
			Description: propertyTopOccurrencesValue,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "TopOccurrencesOccurs"),
			Description: propertyTopOccurrencesOccurs,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getMetaPointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "TopOccurrencesObj"),
		Fields:      getMetaPointingFields,
		Description: propertyTopOccurrences,
	}

	return graphql.NewObject(getMetaPointing)
}

func genMetaClassIntPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaIntFields := graphql.Fields{

		"sum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Sum"),
			Description: propertySum,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Type"),
			Description: propertyType,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"lowest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Lowest"),
			Description: propertyLowest,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"highest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Highest"),
			Description: propertyHighest,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"average": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Average"),
			Description: propertyAverage,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Count"),
			Description: propertyCount,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getMetaIntProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaIntFields,
		Description: propertyObject,
	}

	return graphql.NewObject(getMetaIntProperty)
}

func genMetaClassNumberPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaNumberFields := graphql.Fields{

		"sum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Sum"),
			Description: propertySum,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Type"),
			Description: propertyType,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"lowest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Lowest"),
			Description: propertyLowest,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"highest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Highest"),
			Description: propertyHighest,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"average": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Average"),
			Description: propertyAverage,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Count"),
			Description: propertyCount,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}

	getMetaNumberProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaNumberFields,
		Description: propertyObject,
	}

	return graphql.NewObject(getMetaNumberProperty)
}

func genMetaClassBooleanPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaBooleanFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Type"),
			Description: propertyType,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Count"),
			Description: propertyCount,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},

		"totalTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "TotalTrue"),
			Description: "The amount of times this property's value is true in the dataset",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},

		"percentageTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "PercentageTrue"),
			Description: "Percentage of boolean values that is true",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},
	}

	getMetaBooleanProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaBooleanFields,
		Description: propertyObject,
	}

	return graphql.NewObject(getMetaBooleanProperty)
}

// a duplicate of the string function, this is a separate function to account for future expansions of functionality
func genMetaClassDatePropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	topOccurrencesFields := genMetaClassDatePropertyTopOccurrencesFields(class, property)

	getMetaDateFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Type"),
			Description: propertyType,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Count"),
			Description: propertyCount,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},

		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "TopOccurrences"),
			Description: propertyTopOccurrences,
			Type:        graphql.NewList(topOccurrencesFields),
			Args: graphql.FieldConfigArgument{
				"first": &graphql.ArgumentConfig{
					Description: "Pagination option, show the first x results",
					Type:        graphql.Int,
				},
				"after": &graphql.ArgumentConfig{
					Description: "Pagination option, show the results after the first x results",
					Type:        graphql.Int,
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},
	}

	getMetaDateProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaDateFields,
		Description: propertyObject,
	}

	return graphql.NewObject(getMetaDateProperty)
}

func genMetaClassDatePropertyTopOccurrencesFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaMetaPointingFields := graphql.Fields{

		"value": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "TopOccurrencesValue"),
			Description: propertyTopOccurrencesValue,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},

		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "TopOccurrencesOccurs"),
			Description: propertyTopOccurrencesOccurs,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},
	}

	getMetaMetaPointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "TopOccurrencesObj"),
		Fields:      getMetaMetaPointingFields,
		Description: propertyTopOccurrences,
	}

	return graphql.NewObject(getMetaMetaPointing)
}

func genMetaClassCRefPropertyObj(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) *graphql.Object {
	getMetaCRefPropertyFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Type"),
			Description: propertyType,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Count"),
			Description: propertyCount,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},

		"pointingTo": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "PointingTo"),
			Description: "Which other classes the object property is pointing to",
			Type:        graphql.NewList(graphql.String),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},
	}

	metaClassCRefPropertyConf := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaCRefPropertyFields,
		Description: propertyObject,
	}

	return graphql.NewObject(metaClassCRefPropertyConf)
}

func genMetaPropertyObj(class *models.SemanticSchemaClass) *graphql.Object {
	getMetaPropertyFields := graphql.Fields{

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s", "Meta", class.Class, "MetaCount"),
			Description: "Total amount of found instances",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				result, err := dbConnector.GetGraph(p)
				return result, err
			},
		},
	}

	metaPropertyFields := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s", "Meta", class.Class, "MetaObj"),
		Fields:      getMetaPropertyFields,
		Description: "Meta information about a class object and its (filtered) objects",
	}

	return graphql.NewObject(metaPropertyFields)
}
