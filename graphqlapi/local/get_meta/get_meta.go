package local_get_meta

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
	"strings"
)

func Build(dbSchema *schema.Schema) (*graphql.Field, error) {
	getKinds := graphql.Fields{}

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("There are not any Actions or Things classes defined yet.")
	}

	knownClasses := map[string]*graphql.Object{}

	metaProperties := newMetaProperties("", "") // TODO fix this!

	if len(dbSchema.Actions.Classes) > 0 {
		localGetActions, err := buildGetMetaClasses(dbSchema, kind.ACTION_KIND, dbSchema.Actions, metaProperties, &knownClasses)
		if err != nil {
			return nil, err
		}

		getKinds["Actions"] = &graphql.Field{
			Name:        "WeaviateLocalGetMetaActions",
			Description: "Get Meta information about Actions on the Local Weaviate",
			Type:        localGetActions,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	if len(dbSchema.Things.Classes) > 0 {
		localGetMetaThings, err := buildGetMetaClasses(dbSchema, kind.THING_KIND, dbSchema.Things, metaProperties, &knownClasses)
		if err != nil {
			return nil, err
		}

		getKinds["Things"] = &graphql.Field{
			Name:        "WeaviateLocalGetMetaThings",
			Description: "Get Meta information about Things on the Local Weaviate",
			Type:        localGetMetaThings,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	getMetaField := &graphql.Field{
		Name:        "WeaviateLocalGetMetaObj",
		Description: "Type of Get function to get meta information about Things or Actions on the Local Weaviate",
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "WeaviateLocalGetMetaObj",
			Fields:      getKinds,
			Description: "Type of Get function to get meta information about Things or Actions on the Local Weaviate",
		}),
		Args: graphql.FieldConfigArgument{
			"where": &graphql.ArgumentConfig{
				Description: "Filter options for the GetMeta search, to convert the data to the filter input",
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        "WeaviateLocalGetMetaWhereInpObj",
						Fields:      common_filters.GetGetAndGetMetaWhereFilters(),
						Description: "Filter options for the GetMeta search, to convert the data to the filter input",
					},
				),
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// TODO: not implemented yet.
			return nil, nil
		},
	}

	return getMetaField, nil
}

// Builds the classes below a Local -> Get -> (k kind.Kind)
func buildGetMetaClasses(dbSchema *schema.Schema, k kind.Kind, semanticSchema *models.SemanticSchema, metaProperties MetaProperties, knownClasses *map[string]*graphql.Object) (*graphql.Object, error) {
	classFields := graphql.Fields{}

	var kindName string
	switch k {
	case kind.THING_KIND:
		kindName = "Thing"
	case kind.ACTION_KIND:
		kindName = "Action"
	}

	for _, class := range semanticSchema.Classes {
		classField, err := buildGetMetaClass(dbSchema, k, class, metaProperties, knownClasses)
		if err != nil {
			return nil, fmt.Errorf("Could not build class for %s", class.Class)
		}
		classFields[class.Class] = classField
	}

	classes := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateLocalGetMeta%ssObj", kindName),
		Fields:      classFields,
		Description: fmt.Sprintf("Type of %ss i.e. %ss classes to GetMeta information of on the Local Weaviate", kindName, kindName),
	})

	return classes, nil
}

// Build a single class in Local -> Get -> (k kind.Kind) -> (models.SemanticSchemaClass)
func buildGetMetaClass(dbSchema *schema.Schema, k kind.Kind, class *models.SemanticSchemaClass, metaProperties MetaProperties, knownClasses *map[string]*graphql.Object) (*graphql.Field, error) {
	classObject := graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("Meta%s", class.Class),
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			classProperties := graphql.Fields{}

			classProperties["meta"] = metaProperties.classMeta

			for _, property := range class.Properties {
				propertyType, err := dbSchema.FindPropertyDataType(property.AtDataType)
				if err != nil {
					// We can't return an error in this FieldsThunk function, so we need to panic
					panic(fmt.Sprintf("buildGetMetaClass: wrong propertyType for %s.%s.%s; %s", k.Name(), class.Class, property.Name, err.Error()))
				}

				if propertyType.IsPrimitive() {
					primitiveType := propertyType.AsPrimitive()
					propertyObject := metaProperties.properties[primitiveType]

					if propertyObject == nil {
						panic(fmt.Sprintf("buildGetMetaClass: unknown primitive type for %s.%s.%s; %s", k.Name(), class.Class, property.Name, propertyType.AsPrimitive()))
					}

					classProperties[property.Name] = &graphql.Field{
						Name: fmt.Sprintf("Meta%s%s", class.Class, property.Name),
						Type: propertyObject,
					}
				} else {
					// This is a reference
					refClasses := propertyType.Classes()
					propertyName := strings.Title(property.Name)
					dataTypeClasses := make([]*graphql.Object, len(refClasses))

					for index, refClassName := range refClasses {
						refClass, ok := (*knownClasses)[string(refClassName)]

						if !ok {
							panic(fmt.Sprintf("buildGetMetaClass: unknown referenced class type for %s.%s.%s; %s", k.Name(), class.Class, property.Name, refClassName))
						}

						dataTypeClasses[index] = refClass
					}

					classProperties[propertyName] = &graphql.Field{
						Type:        graphql.String,
						Description: property.Description,
					}
				}
			}

			return classProperties
		}),
		Description: class.Description,
	})

	(*knownClasses)[class.Class] = classObject

	classField := graphql.Field{
		Type:        classObject,
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
			return nil, nil
		},
	}

	return &classField, nil
}
