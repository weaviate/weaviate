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

// Package getmeta provides the network getmeta graphql endpoint for Weaviate
package getmeta

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated GetMeta Things part of the schema
func ClassFieldsFromSchema(databaseSchema []*models.SemanticSchemaClass, classParentTypeIsAction bool, weaviate string) (*graphql.Object, error) {
	classFields := graphql.Fields{}
	kindName := "Thing"
	name := fmt.Sprintf("%s%s%s", "WeaviateNetworkGetMeta", weaviate, "ThingsObj")
	description := descriptions.NetworkGetMetaThingsObjDesc
	if classParentTypeIsAction {
		kindName = "Action"
		name = fmt.Sprintf("%s%s%s", "WeaviateNetworkGetMeta", weaviate, "ActionsObj")
		description = descriptions.NetworkGetMetaActionsObjDesc
	}

	for _, class := range databaseSchema {
		field, err := metaClassField(class, kindName, class.Description, weaviate)

		if err != nil {
			return nil, err
		}

		classFields[class.Class] = field
	}

	classes := graphql.ObjectConfig{
		Name:        name,
		Fields:      classFields,
		Description: description,
	}

	return graphql.NewObject(classes), nil
}

func metaClassField(class *models.SemanticSchemaClass, kindName string, description string, weaviate string) (*graphql.Field, error) {
	metaClassName := fmt.Sprintf("%s%s%s", weaviate, "Meta", class.Class)

	propertyFields := graphql.ObjectConfig{
		Name: metaClassName,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			propertyFields, err := metaClassPropertyFields(class, weaviate)

			if err != nil {
				panic("Failed to assemble single Network Meta Class field")
			}

			return propertyFields
		}),
		Description: description,
	}

	propertyFieldsObject := graphql.NewObject(propertyFields)
	propertyFieldsField := &graphql.Field{
		Type:        propertyFieldsObject,
		Description: description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: descriptions.FirstDesc,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.AfterDesc,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.NetworkGetWhereDesc,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("WeaviateNetworkGetMeta%s%ss%sWhereInpObj", weaviate, kindName, class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateNetworkGetMeta%s%ss%s", weaviate, kindName, class.Class)),
						Description: descriptions.NetworkGetWhereInpObjDesc,
					},
				),
			},
		},
	}

	return propertyFieldsField, nil
}

func metaClassPropertyFields(class *models.SemanticSchemaClass, weaviate string) (graphql.Fields, error) {
	propertyFields := graphql.Fields{}
	metaPropertyObj := propertyObj(class, weaviate)

	metaPropertyObjField := &graphql.Field{
		Description: descriptions.GetMetaMetaPropertyDesc,
		Type:        metaPropertyObj,
	}

	propertyFields["meta"] = metaPropertyObjField

	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)

		if err != nil {
			return nil, err
		}

		convertedDataType, err := handleNonObjectPropertyDataTypes(*propertyType, class, property, weaviate)

		if err != nil {
			return nil, err
		}

		if *propertyType == schema.DataTypeCRef {
			propertyFields[strings.Title(property.Name)] = convertedDataType
		} else {
			propertyFields[property.Name] = convertedDataType
		}
	}

	return propertyFields, nil
}

func handleNonObjectPropertyDataTypes(dataType schema.DataType, class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) (*graphql.Field, error) {
	stringPropertyFields := classStringPropertyFields(class, property, weaviate)
	textPropertyFields := classTextPropertyFields(class, property, weaviate)
	intPropertyFields := classIntPropertyFields(class, property, weaviate)
	numberPropertyFields := classNumberPropertyFields(class, property, weaviate)
	booleanPropertyFields := classBooleanPropertyFields(class, property, weaviate)
	datePropertyFields := classDatePropertyFields(class, property, weaviate)
	cRefPropertyFields := classCRefPropertyObj(class, property, weaviate)

	switch dataType {

	case schema.DataTypeString:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaPropertyDesc, property.Name),
			Type:        stringPropertyFields,
		}, nil

	case schema.DataTypeText:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaPropertyDesc, property.Name),
			Type:        textPropertyFields,
		}, nil

	case schema.DataTypeInt:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaPropertyDesc, property.Name),
			Type:        intPropertyFields,
		}, nil

	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaPropertyDesc, property.Name),
			Type:        numberPropertyFields,
		}, nil

	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaPropertyDesc, property.Name),
			Type:        booleanPropertyFields,
		}, nil

	case schema.DataTypeDate:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaPropertyDesc, property.Name),
			Type:        datePropertyFields,
		}, nil

	case schema.DataTypeCRef:
		return &graphql.Field{
			Description: fmt.Sprintf(`%s"%s"`, descriptions.GetMetaPropertyDesc, property.Name),
			Type:        cRefPropertyFields,
		}, nil

	default:
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype)
	}
}

func classStringPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	topOccurrencesFields := classStringPropertyTopOccurrencesFields(class, property, weaviate)

	getMetaPointingFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", weaviate, "Meta", class.Class, "Type"),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", weaviate, "Meta", class.Class, "Count"),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
		},

		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", weaviate, "Meta", class.Class, "TopOccurrences"),
			Description: descriptions.GetMetaPropertyTopOccurrencesDesc,
			Type:        graphql.NewList(topOccurrencesFields),
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
		},
	}

	getMetaStringProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaPointingFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(getMetaStringProperty)
}

func classStringPropertyTopOccurrencesFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	getMetaPointingFields := graphql.Fields{

		"value": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesValue"),
			Description: descriptions.GetMetaPropertyTopOccurrencesValueDesc,
			Type:        graphql.String,
		},

		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesOccurs"),
			Description: descriptions.GetMetaPropertyTopOccurrencesOccursDesc,
			Type:        graphql.Int,
		},
	}

	getMetaPointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesObj"),
		Fields:      getMetaPointingFields,
		Description: descriptions.GetMetaPropertyTopOccurrencesDesc,
	}

	return graphql.NewObject(getMetaPointing)
}

func classTextPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	topOccurrencesFields := classTextPropertyTopOccurrencesFields(class, property, weaviate)

	getMetaPointingFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", weaviate, "Meta", class.Class, "Type"),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", weaviate, "Meta", class.Class, "Count"),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
		},

		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", weaviate, "Meta", class.Class, "TopOccurrences"),
			Description: descriptions.GetMetaPropertyTopOccurrencesDesc,
			Type:        graphql.NewList(topOccurrencesFields),
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
		},
	}

	getMetaTextProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaPointingFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(getMetaTextProperty)
}

func classTextPropertyTopOccurrencesFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	getMetaPointingFields := graphql.Fields{

		"value": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesValue"),
			Description: descriptions.GetMetaPropertyTopOccurrencesValueDesc,
			Type:        graphql.String,
		},

		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesOccurs"),
			Description: descriptions.GetMetaPropertyTopOccurrencesOccursDesc,
			Type:        graphql.Int,
		},
	}

	getMetaPointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesObj"),
		Fields:      getMetaPointingFields,
		Description: descriptions.GetMetaPropertyTopOccurrencesDesc,
	}

	return graphql.NewObject(getMetaPointing)
}

func classIntPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	getMetaIntFields := graphql.Fields{

		"sum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Sum"),
			Description: descriptions.GetMetaPropertySumDesc,
			Type:        graphql.Float,
		},

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Type"),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},

		"lowest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Lowest"),
			Description: descriptions.GetMetaPropertyLowestDesc,
			Type:        graphql.Float,
		},

		"highest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Highest"),
			Description: descriptions.GetMetaPropertyHighestDesc,
			Type:        graphql.Float,
		},

		"average": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Average"),
			Description: descriptions.GetMetaPropertyAverageDesc,
			Type:        graphql.Float,
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Count"),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
		},
	}

	getMetaIntProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaIntFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(getMetaIntProperty)
}

func classNumberPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	getMetaNumberFields := graphql.Fields{

		"sum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Sum"),
			Description: descriptions.GetMetaPropertySumDesc,
			Type:        graphql.Float,
		},

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Type"),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},

		"lowest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Lowest"),
			Description: descriptions.GetMetaPropertyLowestDesc,
			Type:        graphql.Float,
		},

		"highest": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Highest"),
			Description: descriptions.GetMetaPropertyHighestDesc,
			Type:        graphql.Float,
		},

		"average": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Average"),
			Description: descriptions.GetMetaPropertyAverageDesc,
			Type:        graphql.Float,
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Count"),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
		},
	}

	getMetaNumberProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaNumberFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(getMetaNumberProperty)
}

func classBooleanPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	getMetaBooleanFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Type"),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Count"),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
		},

		"totalTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TotalTrue"),
			Description: descriptions.GetMetaClassPropertyTotalTrueDesc,
			Type:        graphql.Int,
		},

		"percentageTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "PercentageTrue"),
			Description: descriptions.GetMetaClassPropertyPercentageTrueDesc,
			Type:        graphql.Float,
		},

		"totalFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TotalFalse"),
			Description: descriptions.GetMetaClassPropertyTotalFalseDesc,
			Type:        graphql.Int,
		},

		"percentageFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "PercentageFalse"),
			Description: descriptions.GetMetaClassPropertyPercentageFalseDesc,
			Type:        graphql.Float,
		},
	}

	getMetaBooleanProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaBooleanFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(getMetaBooleanProperty)
}

// a duplicate of the string function, this is a separate function to account for future expansions of functionality
func classDatePropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	topOccurrencesFields := classDatePropertyTopOccurrencesFields(class, property, weaviate)

	getMetaDateFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Type"),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Count"),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
		},

		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrences"),
			Description: descriptions.GetMetaPropertyTopOccurrencesDesc,
			Type:        graphql.NewList(topOccurrencesFields),
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
		},
	}

	getMetaDateProperty := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaDateFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(getMetaDateProperty)
}

func classDatePropertyTopOccurrencesFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	getMetaMetaPointingFields := graphql.Fields{

		"value": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesValue"),
			Description: descriptions.GetMetaPropertyTopOccurrencesValueDesc,
			Type:        graphql.String,
		},

		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesOccurs"),
			Description: descriptions.GetMetaPropertyTopOccurrencesOccursDesc,
			Type:        graphql.Int,
		},
	}

	getMetaMetaPointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "TopOccurrencesObj"),
		Fields:      getMetaMetaPointingFields,
		Description: descriptions.GetMetaPropertyTopOccurrencesDesc,
	}

	return graphql.NewObject(getMetaMetaPointing)
}

func classCRefPropertyObj(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty, weaviate string) *graphql.Object {
	getMetaCRefPropertyFields := graphql.Fields{

		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Type"),
			Description: descriptions.GetMetaPropertyTypeDesc,
			Type:        graphql.String,
		},

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Count"),
			Description: descriptions.GetMetaPropertyCountDesc,
			Type:        graphql.Int,
		},

		"pointingTo": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "PointingTo"),
			Description: descriptions.GetMetaClassPropertyPointingToDesc,
			Type:        graphql.NewList(graphql.String),
		},
	}

	cRefPropertyConf := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s%s", weaviate, "Meta", class.Class, property.Name, "Obj"),
		Fields:      getMetaCRefPropertyFields,
		Description: descriptions.GetMetaPropertyObjectDesc,
	}

	return graphql.NewObject(cRefPropertyConf)
}

func propertyObj(class *models.SemanticSchemaClass, weaviate string) *graphql.Object {
	getMetaPropertyFields := graphql.Fields{

		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%s%s", weaviate, "Meta", class.Class, "MetaCount"),
			Description: descriptions.GetMetaClassMetaCountDesc,
			Type:        graphql.Int,
		},
	}

	metaPropertyFields := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s%s", weaviate, "Meta", class.Class, "MetaObj"),
		Fields:      getMetaPropertyFields,
		Description: descriptions.GetMetaClassMetaObjDesc,
	}

	return graphql.NewObject(metaPropertyFields)
}
