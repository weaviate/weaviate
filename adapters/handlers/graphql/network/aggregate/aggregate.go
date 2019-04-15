/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package aggregate

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated Aggregate Things part of the schema
func classFields(databaseSchema []*models.SemanticSchemaClass, k kind.Kind, peerName string) (*graphql.Object, error) {
	fields := graphql.Fields{}

	for _, class := range databaseSchema {
		field, err := classField(peerName, k, class, class.Description)
		if err != nil {
			return nil, err
		}

		fields[class.Class] = field
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateNetworkAggregate%s%ssObj", peerName, k.TitleizedName()),
		Fields:      fields,
		Description: descriptions.NetworkAggregateThingsActionsObj,
	}), nil
}

func classField(peerName string, k kind.Kind, class *models.SemanticSchemaClass, description string) (*graphql.Field, error) {

	if len(class.Properties) == 0 {
		// if we don't have class properties, we can't build this particular class,
		// as it would not have any fields. So we have to return (without an
		// error), so as not to block the creation of other classes
		return nil, nil
	}

	metaClassName := fmt.Sprintf("%sAggregate%s", peerName, class.Class)

	fields := graphql.ObjectConfig{
		Name: metaClassName,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			fields, err := classPropertyFields(peerName, class)
			if err != nil {
				// we cannot return an error in this FieldsThunk and have to panic unfortunately
				panic(fmt.Sprintf("Failed to assemble single Network Aggregate class field: %s", err))
			}

			return fields
		}),
		Description: description,
	}

	fieldsObject := graphql.NewObject(fields)
	fieldsField := &graphql.Field{
		Type:        graphql.NewList(fieldsObject),
		Description: description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: descriptions.First,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.After,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.LocalGetWhere,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name: fmt.Sprintf("WeaviateNetworkAggregate%s%ss%sWhereInpObj",
							peerName, k.TitleizedName(), class.Class),
						Fields: common_filters.BuildNew(fmt.Sprintf("WeaviateNetworkAggregate%s%ss%s",
							peerName, k.TitleizedName(), class.Class)),
						Description: descriptions.LocalGetWhereInpObj,
					},
				),
			},
			"groupBy": &graphql.ArgumentConfig{
				Description: descriptions.GroupBy,
				Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
			},
		},
	}

	return fieldsField, nil
}

func classPropertyFields(peerName string, class *models.SemanticSchemaClass) (graphql.Fields, error) {
	fields := graphql.Fields{}
	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %s", class.Class, property.Name, err)
		}

		convertedDataType, err := classPropertyField(peerName, *propertyType, class, property)
		if err != nil {
			return nil, err
		}

		if *propertyType == schema.DataTypeCRef {
			fields[strings.Title(property.Name)] = convertedDataType
		} else {
			fields[property.Name] = convertedDataType
		}
	}

	// Always append Grouped By field
	fields["groupedBy"] = &graphql.Field{
		Description: descriptions.NetworkAggregateGroupedBy,
		Type:        groupedByProperty(class, peerName),
	}

	return fields, nil
}

func classPropertyField(peerName string, dataType schema.DataType, class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {
	switch dataType {
	case schema.DataTypeString:
		return makePropertyField(peerName, class, property, nonNumericPropertyFields)
	case schema.DataTypeText:
		return makePropertyField(peerName, class, property, nonNumericPropertyFields)
	case schema.DataTypeInt:
		return makePropertyField(peerName, class, property, numericPropertyFields)
	case schema.DataTypeNumber:
		return makePropertyField(peerName, class, property, numericPropertyFields)
	case schema.DataTypeBoolean:
		return makePropertyField(peerName, class, property, nonNumericPropertyFields)
	case schema.DataTypeDate:
		return makePropertyField(peerName, class, property, nonNumericPropertyFields)
	case schema.DataTypeCRef:
		return makePropertyField(peerName, class, property, nonNumericPropertyFields)
	case schema.DataTypeGeoCoordinates:
		// simply skip for now, see gh-729
		return nil, nil
	default:
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype)
	}
}

type propertyFieldMaker func(class *models.SemanticSchemaClass,
	property *models.SemanticSchemaClassProperty, prefix string) *graphql.Object

func makePropertyField(peerName string, class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty,
	fieldMaker propertyFieldMaker) (*graphql.Field, error) {
	prefix := fmt.Sprintf("%sAggregate", peerName)
	return &graphql.Field{
		Description: fmt.Sprintf(`%s"%s"`, descriptions.AggregateProperty, property.Name),
		Type:        fieldMaker(class, property, prefix),
	}, nil
}
