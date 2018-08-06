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
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated MetaFetch Things part of the schema
func (g *GraphQL) genMetaClassFieldsFromSchema(databaseSchema []*models.SemanticSchemaClass, classParentTypeIsAction bool) (*graphql.Object, error) {

	classFields := graphql.Fields{}

	for _, class := range databaseSchema {
		field, err := genMetaSingleClassField(class)

		if err != nil {
			return nil, err
		}
		classFields[class.Class] = field
	}

	name := "WeaviateLocalMetaFetchGenericsThingsObj"
	description := "Thing to fetch for meta generic fetch"
	if classParentTypeIsAction {
		name = "WeaviateLocalMetaFetchGenericsActionsObj"
		description = "Action to fetch for meta generic fetch"
	}
	localMetaFetchClasses := graphql.ObjectConfig{
		Name:        name,
		Fields:      classFields,
		Description: description,
	}

	return graphql.NewObject(localMetaFetchClasses), nil
}

func genMetaSingleClassField(class *models.SemanticSchemaClass) (*graphql.Field, error) {

	metaClassName := mergeStrings("Meta", class.Class)

	singleClassPropertyFields := graphql.ObjectConfig{
		Name: metaClassName,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleClassPropertyFields, err := genMetaSingleClassPropertyFields(class)
			if err != nil {
				panic("oops")
			}
			return singleClassPropertyFields
		}),
		Description: "Type of fetch on the internal Weaviate",
	}

	singleClassPropertyFieldsObject := graphql.NewObject(singleClassPropertyFields)
	singleClassPropertyFieldsField := &graphql.Field{
		Type:        singleClassPropertyFieldsObject,
		Description: class.Description,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}
	return singleClassPropertyFieldsField, nil
}

// TODO: replace "obj" variable name with an actual name
func genMetaSingleClassPropertyFields(class *models.SemanticSchemaClass) (graphql.Fields, error) {

	singleClassPropertyFields := graphql.Fields{}

	metaPropertyObj, err := genMetaPropertyObj(class)
	if err != nil {
		return nil, fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	}
	metaPropertyObjField := &graphql.Field{
		Description: "meta information about class object",
		Type:        metaPropertyObj,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}
	singleClassPropertyFields["Meta"] = metaPropertyObjField

	for _, property := range class.Properties {

		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, err
		}

		convertedDataType, err := handleMetaFetchNonObjectDataTypes(*propertyType, class, property)

		if err != nil {
			return nil, err
		}
		singleClassPropertyFields[property.Name] = convertedDataType
	}
	return singleClassPropertyFields, nil
}

func handleMetaFetchNonObjectDataTypes(dataType schema.DataType, class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {

	metaClassStringPropertyFields, err := genMetaClassStringPropertyFields(class, property)
	if err != nil {
		return nil, err
	}
	metaClassIntPropertyFields, err := genMetaClassIntPropertyFields(class, property)
	if err != nil {
		return nil, err
	}
	metaClassNumberPropertyFields, err := genMetaClassNumberPropertyFields(class, property)
	if err != nil {
		return nil, err
	}
	metaClassBooleanPropertyFields, err := genMetaClassBooleanPropertyFields(class, property)
	if err != nil {
		return nil, err
	}
	metaClassDatePropertyFields, err := genMetaClassDatePropertyFields(class, property)
	if err != nil {
		return nil, err
	}
	metaClassCRefPropertyFields, err := genMetaClassCRefPropertyObj(class, property)
	if err != nil {
		return nil, err
	}

	switch dataType {

	case schema.DataTypeString:
		return &graphql.Field{
			Description: mergeStrings("Meta information about the property \"", property.Name, "\""),
			Type:        metaClassStringPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeInt:
		return &graphql.Field{
			Description: mergeStrings("Meta information about the property \"", property.Name, "\""),
			Type:        metaClassIntPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: mergeStrings("Meta information about the property \"", property.Name, "\""),
			Type:        metaClassNumberPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: mergeStrings("Meta information about the property \"", property.Name, "\""),
			Type:        metaClassBooleanPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeDate:
		return &graphql.Field{
			Description: mergeStrings("Meta information about the property \"", property.Name, "\""),
			Type:        metaClassDatePropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeCRef:
		return &graphql.Field{
			Description: property.Description,
			Type:        metaClassCRefPropertyFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	default:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
		}, fmt.Errorf(schema.ErrorNoSuchDatatype)
	}
}

func genMetaClassStringPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	topOccurencesFields, err := genMetaClassStringPropertyTopOccurrencesFields(class, property)
	if err != nil {
		return nil, err
	}
	metaFetchMetaPointingFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointingTo"),
			Description: "datatype of the property",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"counter": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointingFrom"),
			Description: "total amount of found instances",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"topOccurences": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointingFrom"),
			Description: "most frequent property values",
			Type:        topOccurencesFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchStringProperty := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "Obj"),
		Fields:      metaFetchMetaPointingFields,
		Description: "Property meta information",
	}
	return graphql.NewObject(metaFetchStringProperty), nil
}

func genMetaClassStringPropertyTopOccurrencesFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	metaFetchMetaPointingFields := graphql.Fields{
		"value": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "TopOccurencesValue"),
			Description: "property value of the most frequent properties",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"occurs": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "TopOccurencesOccurs"),
			Description: "number of occurrance",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchMetaPointing := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "TopOccurencesObj"),
		Fields:      metaFetchMetaPointingFields,
		Description: "most frequent property values",
	}
	return graphql.NewObject(metaFetchMetaPointing), nil
}

func genMetaClassIntPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	metaFetchMetaIntFields := graphql.Fields{
		"sum": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "Sum"),
			Description: "sum of values of found instances",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"type": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Type"),
			Description: "datatype of the property",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"lowest": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Lowest"),
			Description: "Lowest value occurence",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"highest": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Highest"),
			Description: "Highest value occurence",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"average": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Average"),
			Description: "average number",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"counter": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Counter"),
			Description: "total amount of found instances",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchIntProperty := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "Obj"),
		Fields:      metaFetchMetaIntFields,
		Description: "Property meta information",
	}
	return graphql.NewObject(metaFetchIntProperty), nil
}

// a duplicate of the int function, this is a separate function to account for future expansions of functionality
func genMetaClassNumberPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	metaFetchMetaNumberFields := graphql.Fields{
		"sum": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "Sum"),
			Description: "sum of values of found instances",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"type": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Type"),
			Description: "datatype of the property",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"lowest": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Lowest"),
			Description: "Lowest value occurence",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"highest": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Highest"),
			Description: "Highest value occurence",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"average": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Average"),
			Description: "average number",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"counter": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Counter"),
			Description: "total amount of found instances",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchNumberProperty := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "Obj"),
		Fields:      metaFetchMetaNumberFields,
		Description: "Property meta information",
	}
	return graphql.NewObject(metaFetchNumberProperty), nil
}

func genMetaClassBooleanPropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	metaFetchMetaBooleanFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Type"),
			Description: "datatype of the property",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"counter": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Counter"),
			Description: "total amount of found instances",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"totalTrue": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Highest"),
			Description: "total amount of boolean value is true",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"percentageTrue": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "Average"),
			Description: "percentage of boolean = true",
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchBooleanProperty := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "Obj"),
		Fields:      metaFetchMetaBooleanFields,
		Description: "Property meta information",
	}
	return graphql.NewObject(metaFetchBooleanProperty), nil
}

// a duplicate of the string function, this is a separate function to account for future expansions of functionality
func genMetaClassDatePropertyFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	topOccurencesFields, err := genMetaClassDatePropertyTopOccurrencesFields(class, property)
	if err != nil {
		return nil, err
	}
	metaFetchDatePointingFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointingTo"),
			Description: "datatype of the property",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"counter": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointingFrom"),
			Description: "total amount of found instances",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"topOccurences": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointingFrom"),
			Description: "most frequent property values",
			Type:        topOccurencesFields,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchDateProperty := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "Obj"),
		Fields:      metaFetchDatePointingFields,
		Description: "Property meta information",
	}
	return graphql.NewObject(metaFetchDateProperty), nil
}

func genMetaClassDatePropertyTopOccurrencesFields(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	metaFetchMetaPointingFields := graphql.Fields{
		"value": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "TopOccurencesValue"),
			Description: "property value of the most frequent properties",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"occurs": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "TopOccurencesOccurs"),
			Description: "number of occurrance",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchMetaPointing := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "TopOccurencesObj"),
		Fields:      metaFetchMetaPointingFields,
		Description: "most frequent property values",
	}
	return graphql.NewObject(metaFetchMetaPointing), nil
}

func genMetaClassCRefPropertyObj(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	metaCRefPointingObj, err := genMetaCRefPointingObj(class, property)
	if err != nil {
		return nil, fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	}

	metaFetchCRefPropertyFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "PointingTo"),
			Description: "datatype of the property",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"counter": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "Counter"),
			Description: "total amount of found instances",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"pointing": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, property.Name, "Pointing"),
			Description: "pointing to and from how many other things",
			Type:        metaCRefPointingObj,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}

	metaClassCRefPropertyConf := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "Obj"),
		Fields:      metaFetchCRefPropertyFields,
		Description: "meta information about class object",
	}

	return graphql.NewObject(metaClassCRefPropertyConf), nil
}

func genMetaCRefPointingObj(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Object, error) {

	metaFetchCRefPointingFields := graphql.Fields{
		"to": &graphql.Field{
			Description: "how many other classes the class is pointing to",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"from": &graphql.Field{
			Description: "how many other classes the class is pointing from",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchCRefPointing := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, property.Name, "PointingObj"),
		Fields:      metaFetchCRefPointingFields,
		Description: "pointing to and from how many other things",
	}
	return graphql.NewObject(metaFetchCRefPointing), nil
}

func genMetaPropertyObj(class *models.SemanticSchemaClass) (*graphql.Object, error) {

	metaPointingObj, err := genMetaPointingObj(class)
	if err != nil {
		return nil, fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	}

	metaFetchMetaPropertyFields := graphql.Fields{
		"counter": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaCounter"),
			Description: "how many class instances are there",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"pointing": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointing"),
			Description: "pointing to and from how many other things",
			Type:        metaPointingObj,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}

	metaPropertyFields := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, "MetaObj"),
		Fields:      metaFetchMetaPropertyFields,
		Description: "meta information about class object",
	}

	return graphql.NewObject(metaPropertyFields), nil
}

func genMetaPointingObj(class *models.SemanticSchemaClass) (*graphql.Object, error) {

	metaFetchMetaPointingFields := graphql.Fields{
		"to": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointingTo"),
			Description: "how many other classes the class is pointing to",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"from": &graphql.Field{
			Name:        mergeStrings("Meta", class.Class, "MetaPointingFrom"),
			Description: "how many other classes the class is pointing from",
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchMetaPointing := graphql.ObjectConfig{
		Name:        mergeStrings("Meta", class.Class, "MetaPointingObj"),
		Fields:      metaFetchMetaPointingFields,
		Description: "pointing to and from how many other things",
	}
	return graphql.NewObject(metaFetchMetaPointing), nil
}
