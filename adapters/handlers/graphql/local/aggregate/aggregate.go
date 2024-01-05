//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package aggregate

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/utils"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

type ModulesProvider interface {
	AggregateArguments(class *models.Class) map[string]*graphql.ArgumentConfig
	ExtractSearchParams(arguments map[string]interface{}, className string) map[string]interface{}
}

// Build the Aggregate Kinds schema
func Build(dbSchema *schema.Schema, config config.Config,
	modulesProvider ModulesProvider,
) (*graphql.Field, error) {
	if len(dbSchema.Objects.Classes) == 0 {
		return nil, utils.ErrEmptySchema
	}

	var err error
	var localAggregateObjects *graphql.Object
	if len(dbSchema.Objects.Classes) > 0 {
		localAggregateObjects, err = classFields(dbSchema.Objects.Classes, config, modulesProvider)
		if err != nil {
			return nil, err
		}
	}

	field := graphql.Field{
		Name:        "Aggregate",
		Description: descriptions.AggregateWhere,
		Type:        localAggregateObjects,
		Resolve:     passThroughResolver,
	}

	return &field, nil
}

func classFields(databaseSchema []*models.Class,
	config config.Config, modulesProvider ModulesProvider,
) (*graphql.Object, error) {
	fields := graphql.Fields{}

	for _, class := range databaseSchema {
		field, err := classField(class, class.Description, config, modulesProvider)
		if err != nil {
			return nil, err
		}

		fields[class.Class] = field
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        "AggregateObjectsObj",
		Fields:      fields,
		Description: descriptions.AggregateObjectsObj,
	}), nil
}

func classField(class *models.Class, description string,
	config config.Config, modulesProvider ModulesProvider,
) (*graphql.Field, error) {
	metaClassName := fmt.Sprintf("Aggregate%s", class.Class)

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
			"limit": &graphql.ArgumentConfig{
				Description: descriptions.First,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.GetWhere,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("AggregateObjects%sWhereInpObj", class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("AggregateObjects%s", class.Class)),
						Description: descriptions.GetWhereInpObj,
					},
				),
			},
			"groupBy": &graphql.ArgumentConfig{
				Description: descriptions.GroupBy,
				Type:        graphql.NewList(graphql.String),
			},
			"nearVector": nearVectorArgument(class.Class),
			"nearObject": nearObjectArgument(class.Class),
			"objectLimit": &graphql.ArgumentConfig{
				Description: descriptions.First,
				Type:        graphql.Int,
			},
			"hybrid": hybridArgument(fieldsObject, class, modulesProvider),
		},
		Resolve: makeResolveClass(modulesProvider, class),
	}

	if modulesProvider != nil {
		for name, argument := range modulesProvider.AggregateArguments(class) {
			fieldsField.Args[name] = argument
		}
	}

	if schema.MultiTenancyEnabled(class) {
		fieldsField.Args["tenant"] = tenantArgument()
	}

	return fieldsField, nil
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

		fields[property.Name] = convertedDataType
	}

	// Special case: meta { count } appended to all regular props
	fields["meta"] = &graphql.Field{
		Description: descriptions.LocalMetaObj,
		Type:        metaObject(fmt.Sprintf("Aggregate%s", class.Class)),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// pass-through
			return p.Source, nil
		},
	}

	// Always append Grouped By field
	fields["groupedBy"] = &graphql.Field{
		Description: descriptions.AggregateGroupedBy,
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
	case schema.DataTypeText:
		return makePropertyField(class, property, stringPropertyFields)
	case schema.DataTypeInt:
		return makePropertyField(class, property, numericPropertyFields)
	case schema.DataTypeNumber:
		return makePropertyField(class, property, numericPropertyFields)
	case schema.DataTypeBoolean:
		return makePropertyField(class, property, booleanPropertyFields)
	case schema.DataTypeDate:
		return makePropertyField(class, property, datePropertyFields)
	case schema.DataTypeCRef:
		return makePropertyField(class, property, referencePropertyFields)
	case schema.DataTypeGeoCoordinates:
		// simply skip for now, see gh-729
		return nil, nil
	case schema.DataTypePhoneNumber:
		// skipping for now, see gh-1088 where it was outscoped
		return nil, nil
	case schema.DataTypeBlob:
		return makePropertyField(class, property, stringPropertyFields)
	case schema.DataTypeTextArray:
		return makePropertyField(class, property, stringPropertyFields)
	case schema.DataTypeIntArray, schema.DataTypeNumberArray:
		return makePropertyField(class, property, numericPropertyFields)
	case schema.DataTypeBooleanArray:
		return makePropertyField(class, property, booleanPropertyFields)
	case schema.DataTypeDateArray:
		return makePropertyField(class, property, datePropertyFields)
	case schema.DataTypeUUID, schema.DataTypeUUIDArray:
		// not aggregatable
		return nil, nil
	case schema.DataTypeObject, schema.DataTypeObjectArray:
		// TODO: check if it's aggregable, skip for now
		return nil, nil
	default:
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype+": %s", dataType)
	}
}

type propertyFieldMaker func(class *models.Class,
	property *models.Property, prefix string) *graphql.Object

func makePropertyField(class *models.Class, property *models.Property,
	fieldMaker propertyFieldMaker,
) (*graphql.Field, error) {
	prefix := "Aggregate"
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
