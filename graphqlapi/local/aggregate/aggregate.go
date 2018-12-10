package aggregate

import (
	"fmt"
	"strings"
	
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/graphql-go/graphql"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/models"
)

func Build(dbSchema *schema.Schema) (*graphql.Field, error) {
	getKinds := graphql.Fields{}

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("There are no Actions or Things classes defined yet.")
	}

	knownClasses := map[string]*graphql.Object{}

	if len(dbSchema.Actions.Classes) > 0 {
		localAggregateActions, err := buildAggregateClasses(dbSchema, kind.ACTION_KIND, dbSchema.Actions, &knownClasses)
		if err != nil {
			return nil, err
		}

		getKinds["Actions"] = &graphql.Field{
			Name:        "WeaviateLocalAggregateActions",
			Description: descriptions.LocalAggregateActionsDesc,
			Type:        localAggregateActions,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}
	}

	if len(dbSchema.Things.Classes) > 0 {
		localAggregateThings, err := buildAggregateClasses(dbSchema, kind.THING_KIND, dbSchema.Things, &knownClasses)
		if err != nil {
			return nil, err
		}

		getKinds["Things"] = &graphql.Field{
			Name:        "WeaviateLocalAggregateThings",
			Description: descriptions.LocalAggregateThingsDesc,
			Type:        localAggregateThings,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}
	}

	field := graphql.Field{
		Name:        "WeaviateLocalAggregate",
		Description: descriptions.LocalAggregateWhereDesc,
		Args: graphql.FieldConfigArgument{
			"where": &graphql.ArgumentConfig{
				Description: descriptions.LocalGetWhereDesc,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        "WeaviateLocalAggregateWhereInpObj",
						Fields:      common_filters.GetGetAndGetMetaWhereFilters(),
						Description: descriptions.LocalAggregateWhereInpObjDesc,
					},
				),
			},
		},
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "WeaviateLocalAggregateObj",
			Fields:      getKinds,
			Description: descriptions.LocalAggregateObjDesc,
		}),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return &field, nil
}

// Builds the classes below a Local -> Aggregate -> (k kind.Kind)
func buildAggregateClasses(dbSchema *schema.Schema, k kind.Kind, semanticSchema *models.SemanticSchema, knownClasses *map[string]*graphql.Object) (*graphql.Object, error) {
	classFields := graphql.Fields{}

	var kindName string
	switch k {
	case kind.THING_KIND:
		kindName = "Thing"
	case kind.ACTION_KIND:
		kindName = "Action"
	}

	for _, class := range semanticSchema.Classes {
		classField, err := buildAggregateClass(dbSchema, k, class, knownClasses)
		if err != nil {
			return nil, fmt.Errorf("Could not build class for %s", class.Class)
		}
		classFields[class.Class] = classField
	}

	classes := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateLocalAggregate%ssObj", kindName),
		Fields:      classFields,
		Description: fmt.Sprintf(descriptions.LocalAggregateThingsActionsObjDesc, kindName),
	})

	return classes, nil
}

// Build a single class in Local -> Aggregate -> (k kind.Kind) -> (models.SemanticSchemaClass)
func buildAggregateClass(dbSchema *schema.Schema, k kind.Kind, class *models.SemanticSchemaClass, knownClasses *map[string]*graphql.Object) (*graphql.Field, error) {
	classObject := graphql.NewObject(graphql.ObjectConfig{
			
		Name: fmt.Sprintf("Aggregate%s", class.Class),
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
				
			classProperties := graphql.Fields{}

			// only generate these fields if the class contains a numeric property
			if classContainsNumericProperties(dbSchema, class) == true {
				classProperties["sum"] = &graphql.Field{
					Description: descriptions.LocalAggregateSumDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "sum"),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["mode"] = &graphql.Field{
					Description: descriptions.LocalAggregateModeDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "mode"),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["mean"] = &graphql.Field{
					Description: descriptions.LocalAggregateMeanDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "mean"),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["median"] = &graphql.Field{
					Description: descriptions.LocalAggregateMedianDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "median"),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["minimum"] = &graphql.Field{
					Description: descriptions.LocalAggregateMinDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "minimum"),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				classProperties["maximum"] = &graphql.Field{
					Description: descriptions.LocalAggregateMaxDesc,
					Type:        generateNumericPropertyFields(dbSchema, class, "maximum"),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
			}

			// always generate these fields
			classProperties["count"] = &graphql.Field{
				Description: descriptions.LocalAggregateCountDesc,
				Type:        generateCountPropertyFields(dbSchema, class, "count"),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("not supported")
				},
			}
			classProperties["groupedBy"] = &graphql.Field{
				Description: descriptions.LocalAggregateGroupedByDesc,
				Type:        generateGroupedByPropertyFields(dbSchema, class, "groupedBy"),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("not supported")
				},
			}
			
			return classProperties
		}),
		Description: class.Description,
	})

	(*knownClasses)[class.Class] = classObject

	classField := graphql.Field {
		Type:        graphql.NewList(classObject),
		Description: class.Description,
		Args: graphql.FieldConfigArgument {
			"first": &graphql.ArgumentConfig {
				Description: descriptions.FirstDesc,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig {
				Description: descriptions.AfterDesc,
				Type:        graphql.Int,
			},
			"groupBy": &graphql.ArgumentConfig {
				Description: descriptions.GroupByDesc,
				Type:        graphql.NewList(graphql.String),
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return &classField, nil
}


// classContainsNumericProperties determines whether a specified class contains one or more numeric properties.
func classContainsNumericProperties (dbSchema *schema.Schema, class *models.SemanticSchemaClass) bool {
	
	for _, property := range class.Properties {
		propertyType, err := dbSchema.FindPropertyDataType(property.AtDataType)
		
		if err != nil {
			// We can't return an error in this FieldsThunk function, so we need to panic
			panic(fmt.Sprintf("buildGetClass: wrong propertyType for %s.%s; %s", class.Class, property.Name, err.Error()))
		}
		
		if propertyType.IsPrimitive() {
			primitivePropertyType := propertyType.AsPrimitive()
			
			if primitivePropertyType == schema.DataTypeInt || primitivePropertyType == schema.DataTypeNumber {
				return true
			}
		}
	}
	return false
}

func generateNumericPropertyFields(dbSchema *schema.Schema, class *models.SemanticSchemaClass, method string) *graphql.Object {
	classProperties := graphql.Fields{}
	
	for _, property := range class.Properties {
		var propertyField *graphql.Field
		propertyType, err := dbSchema.FindPropertyDataType(property.AtDataType)
		
		if err != nil {
			// We can't return an error in this FieldsThunk function, so we need to panic
			panic(fmt.Sprintf("buildAggregateClass: wrong propertyType for %s.%s; %s", class.Class, property.Name, err.Error()))
		}
		
		if propertyType.IsPrimitive() {
			primitivePropertyType := propertyType.AsPrimitive()
			
			if primitivePropertyType == schema.DataTypeInt || primitivePropertyType == schema.DataTypeNumber {
				propertyField = &graphql.Field{
					Description: property.Description,
					Type:        graphql.Int,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return nil, fmt.Errorf("not supported")
					},
				}
				
				propertyField.Name = property.Name
				classProperties[property.Name] = propertyField
			}
		}
	}
	
	classPropertiesObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Aggregate%s%sObj", class.Class, strings.Title(method)),
		Fields:      classProperties,
		Description: fmt.Sprintf(descriptions.LocalAggregateNumericObj, method),
	})
	
	return classPropertiesObj
}

func generateCountPropertyFields(dbSchema *schema.Schema, class *models.SemanticSchemaClass, method string) *graphql.Object {
	classProperties := graphql.Fields{}
	
	for _, property := range class.Properties {
		propertyField := &graphql.Field{
			Description: property.Description,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		}
		
		propertyField.Name = property.Name
		classProperties[property.Name] = propertyField
	}
	
	classPropertiesObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Aggregate%s%sObj", class.Class, strings.Title(method)),
		Fields:      classProperties,
		Description: descriptions.LocalAggregateCountObj,
	})
	
	return classPropertiesObj
}

func generateGroupedByPropertyFields(dbSchema *schema.Schema, class *models.SemanticSchemaClass, method string) *graphql.Object {
	classProperties := graphql.Fields{
	
		"path": &graphql.Field{
			Description: descriptions.LocalAggregateGroupedByGroupedByPathDesc,
			Type:        graphql.NewList(graphql.String),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
		"value": &graphql.Field{
			Description: descriptions.LocalAggregateGroupedByGroupedByValueDesc,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("not supported")
			},
		},
	}
	
	classPropertiesObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Aggregate%s%sObj", class.Class, strings.Title(method)),
		Fields:      classProperties,
		Description: descriptions.LocalAggregateGroupedByObjDesc,
	})
	
	return classPropertiesObj
}
