//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"fmt"
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
)

// Build the Aggreate Kinds schema
func Build(dbSchema *schema.Schema, config config.Config) (*graphql.Field, error) {
	getKinds := graphql.Fields{}

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("there are no Actions or Things classes defined yet")
	}

	if len(dbSchema.Actions.Classes) > 0 {
		localAggregateActions, err := classFields(dbSchema.Actions.Classes, kind.Action, config)
		if err != nil {
			return nil, err
		}

		getKinds["Actions"] = &graphql.Field{
			Name:        "AggregateActions",
			Description: descriptions.LocalAggregateActions,
			Type:        localAggregateActions,
			Resolve:     passThroughResolver,
		}
	}

	if len(dbSchema.Things.Classes) > 0 {
		localAggregateThings, err := classFields(dbSchema.Things.Classes, kind.Thing, config)
		if err != nil {
			return nil, err
		}

		getKinds["Things"] = &graphql.Field{
			Name:        "AggregateThings",
			Description: descriptions.LocalAggregateThings,
			Type:        localAggregateThings,
			Resolve:     passThroughResolver,
		}
	}

	field := graphql.Field{
		Name:        "Aggregate",
		Description: descriptions.LocalAggregateWhere,
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "AggregateObj",
			Fields:      getKinds,
			Description: descriptions.LocalAggregateObj,
		}),
		Resolve: passThroughResolver,
	}

	return &field, nil
}

func classFields(databaseSchema []*models.Class, k kind.Kind,
	config config.Config) (*graphql.Object, error) {
	fields := graphql.Fields{}

	for _, class := range databaseSchema {
		field, err := classField(k, class, class.Description, config)
		if err != nil {
			return nil, err
		}

		fields[class.Class] = field
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Aggregate%ssObj", k.TitleizedName()),
		Fields:      fields,
		Description: descriptions.LocalAggregateThingsActionsObj,
	}), nil
}

func classField(k kind.Kind, class *models.Class, description string,
	config config.Config) (*graphql.Field, error) {

	if len(class.Properties) == 0 {
		// if we don't have class properties, we can't build this particular class,
		// as it would not have any fields. So we have to return (without an
		// error), so as not to block the creation of other classes
		return nil, nil
	}

	metaClassName := fmt.Sprintf("LocalAggregate%s", class.Class)

	fields := graphql.ObjectConfig{
		Name: metaClassName,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			fields, err := classPropertyFields(class)
			if err != nil {
				// we cannot return an error in this FieldsThunk and have to panic unfortunately
				panic(fmt.Sprintf("Failed to assemble single Local Aggregate Class field: %s", err))
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
				Description: descriptions.GetWhere,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name: fmt.Sprintf("Aggregate%ss%sWhereInpObj",
							k.TitleizedName(), class.Class),
						Fields: common_filters.BuildNew(fmt.Sprintf("Aggregate%ss%s",
							k.TitleizedName(), class.Class)),
						Description: descriptions.GetWhereInpObj,
					},
				),
			},
			"groupBy": &graphql.ArgumentConfig{
				Description: descriptions.GroupBy,
				Type:        graphql.NewList(graphql.String),
			},
		},
		Resolve: makeResolveClass(k),
	}

	fieldsField = extendArgsWithAnalyticsConfig(fieldsField, config)
	return fieldsField, nil
}

func extendArgsWithAnalyticsConfig(field *graphql.Field, config config.Config) *graphql.Field {
	if !config.AnalyticsEngine.Enabled {
		return field
	}

	field.Args["useAnalyticsEngine"] = &graphql.ArgumentConfig{
		DefaultValue: config.AnalyticsEngine.DefaultUseAnalyticsEngine,
		Type:         graphql.Boolean,
	}

	field.Args["forceRecalculate"] = &graphql.ArgumentConfig{
		DefaultValue: false,
		Type:         graphql.Boolean,
	}

	return field
}

func classPropertyFields(class *models.Class) (graphql.Fields, error) {
	fields := graphql.Fields{}
	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %s", class.Class, property.Name, err)
		}

		convertedDataType, err := classPropertyField(*propertyType, class, property)
		if err != nil {
			return nil, err
		}

		if *propertyType == schema.DataTypeCRef {
			fields[strings.Title(property.Name)] = convertedDataType
		} else {
			fields[property.Name] = convertedDataType
		}
	}

	// Special case: meta { count } appended to all regular props
	fields["meta"] = &graphql.Field{
		Description: descriptions.LocalMetaObj,
		Type:        metaObject(fmt.Sprintf("LocalAggregate%s", class.Class)),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// pass-through
			return p.Source, nil
		},
	}

	// Always append Grouped By field
	fields["groupedBy"] = &graphql.Field{
		Description: descriptions.LocalAggregateGroupedBy,
		Type:        groupedByProperty(class),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			switch typed := p.Source.(type) {
			case aggregation.Group:
				return typed.GroupedBy, nil
			case map[string]interface{}:
				return typed["groupedBy"], nil
			default:
				return nil, fmt.Errorf("groupedBy: unsupported type %T", p.Source)
			}
		},
	}

	return fields, nil
}

func metaObject(prefix string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%sMetaObject", prefix),
		Fields: graphql.Fields{
			"count": &graphql.Field{
				Type: graphql.Int,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					group, ok := p.Source.(aggregation.Group)
					if !ok {
						return nil, fmt.Errorf("meta count: expected aggregation.Group, got %T", p.Source)
					}

					return group.Count, nil
				},
			},
		},
	})
}

func classPropertyField(dataType schema.DataType, class *models.Class, property *models.Property) (*graphql.Field, error) {
	switch dataType {
	case schema.DataTypeString:
		return makePropertyField(class, property, stringPropertyFields)
	case schema.DataTypeText:
		return makePropertyField(class, property, stringPropertyFields)
	case schema.DataTypeInt:
		return makePropertyField(class, property, numericPropertyFields)
	case schema.DataTypeNumber:
		return makePropertyField(class, property, numericPropertyFields)
	case schema.DataTypeBoolean:
		return makePropertyField(class, property, booleanPropertyFields)
	case schema.DataTypeDate:
		return makePropertyField(class, property, nonNumericPropertyFields)
	case schema.DataTypeCRef:
		return makePropertyField(class, property, referencePropertyFields)
	case schema.DataTypeGeoCoordinates:
		// simply skip for now, see gh-729
		return nil, nil
	default:
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype+": %s", dataType)
	}
}

type propertyFieldMaker func(class *models.Class,
	property *models.Property, prefix string) *graphql.Object

func makePropertyField(class *models.Class, property *models.Property,
	fieldMaker propertyFieldMaker) (*graphql.Field, error) {
	prefix := "LocalAggregate"
	return &graphql.Field{
		Description: fmt.Sprintf(`%s"%s"`, descriptions.AggregateProperty, property.Name),
		Type:        fieldMaker(class, property, prefix),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			switch typed := p.Source.(type) {
			case aggregation.Group:
				res, ok := typed.Properties[property.Name]
				if !ok {
					return nil, fmt.Errorf("missing property '%s'", property.Name)
				}

				return res, nil

			default:
				return nil, fmt.Errorf("property %s, unsupported type %T", property.Name, p.Source)
			}
		},
	}, nil
}

func passThroughResolver(p graphql.ResolveParams) (interface{}, error) {
	// bubble up root resolver
	return p.Source, nil
}
