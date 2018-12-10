package local_get_meta

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/graphql-go/graphql"
)

type MetaProperties struct {
	properties map[schema.DataType]*graphql.Object
	classMeta  *graphql.Field
}

func newMetaProperties(className string, propertyName string) MetaProperties {
	countProp := &graphql.Field{
		Type:        graphql.Int,
		Description: "Number of instances that have defined this property",
	}

	datatypeProp := &graphql.Field{
		Type:        graphql.String,
		Description: "Datatype of the property",
	}

	return MetaProperties{
		classMeta: &graphql.Field{
			Description: "Meta information about a class object and its (filtered) objects",
			Type: graphql.NewObject(graphql.ObjectConfig{
				Name:        fmt.Sprintf("Meta%sMetaObj", className),
				Description: "Meta information about a class object and its (filtered) objects",
				Fields: graphql.Fields{
					"count": &graphql.Field{
						Description: "Total amount of found instances",
						Type:        graphql.Int,
					},
				},
			}),
		},
		properties: map[schema.DataType]*graphql.Object{

			schema.DataTypeString: graphql.NewObject(graphql.ObjectConfig{
				Name:        fmt.Sprintf("Meta%s%sObj", className, propertyName),
				Description: "Meta information about a string",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"topOccurrences": &graphql.Field{
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

						Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("Meta%s%sTopOccurrencesObj", className, propertyName),
							Fields: graphql.Fields{
								"value": &graphql.Field{
									Description: "The most frequently occurring value of this property in the dataset",
									Type:        graphql.String,
								},
								"occurs": &graphql.Field{
									Description: "Number of occurrence of this property value",
									Type:        graphql.Int,
								},
							},
						}))},
				},
			}),
			
			schema.DataTypeText: graphql.NewObject(graphql.ObjectConfig{
				Name:        fmt.Sprintf("Meta%s%sObj", className, propertyName),
				Description: "Meta information about a text",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"topOccurrences": &graphql.Field{
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

						Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("Meta%s%sTopOccurrencesObj", className, propertyName),
							Fields: graphql.Fields{
								"value": &graphql.Field{
									Description: "The most frequently occurring value of this property in the dataset",
									Type:        graphql.String,
								},
								"occurs": &graphql.Field{
									Description: "Number of occurrence of this property value",
									Type:        graphql.Int,
								},
							},
						}))},
				},
			}),

			schema.DataTypeNumber: graphql.NewObject(graphql.ObjectConfig{
				Name:        fmt.Sprintf("Meta%s%sObj", className, propertyName),
				Description: "Meta information about a Number",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"lowest": &graphql.Field{
						Description: "Lowest value found in the dataset for this property",
						Type:        graphql.Float,
					},
					"highest": &graphql.Field{
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Float,
					},
					"average": &graphql.Field{
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Float,
					},
					"sum": &graphql.Field{
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Float,
					},
				},
			}),

			schema.DataTypeInt: graphql.NewObject(graphql.ObjectConfig{
				Name:        fmt.Sprintf("Meta%s%sObj", className, propertyName),
				Description: "Meta information about a Int",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"lowest": &graphql.Field{
						Description: "Lowest value found in the dataset for this property",
						Type:        graphql.Int,
					},
					"highest": &graphql.Field{
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Int,
					},
					"average": &graphql.Field{
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Float,
					},
					"sum": &graphql.Field{
						Description: "Highest value found in the dataset for this property",
						Type:        graphql.Int,
					},
				},
			}),

			schema.DataTypeBoolean: graphql.NewObject(graphql.ObjectConfig{
				Name:        fmt.Sprintf("Meta%s%sObj", className, propertyName),
				Description: "Meta information about a Boolean",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"totalTrue": &graphql.Field{
						Description: "The amount of times this property's value is true in the dataset",
						Type:        graphql.Int,
					},
					"percentageTrue": &graphql.Field{
						Description: "Percentage of boolean values that is true",
						Type:        graphql.Float,
					},
					"totalFalse": &graphql.Field{
						Description: "The amount of times this property's value is true in the dataset",
						Type:        graphql.Int,
					},
					"percentageFalse": &graphql.Field{
						Description: "Percentage of boolean values that is true",
						Type:        graphql.Float,
					},
				},
			}),

			schema.DataTypeDate: graphql.NewObject(graphql.ObjectConfig{
				Name:        fmt.Sprintf("Meta%s%sObj", className, propertyName),
				Description: "Meta information about a date",
				Fields: graphql.Fields{
					"count": countProp,
					"type":  datatypeProp,
					"topOccurrences": &graphql.Field{
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

						Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("Meta%s%sTopOccurrencesObj", className, propertyName),
							Fields: graphql.Fields{
								"value": &graphql.Field{
									Description: "The most frequently occurring value of this property in the dataset",
									Type:        graphql.String,
								},
								"occurs": &graphql.Field{
									Description: "Number of occurrence of this property value",
									Type:        graphql.Int,
								},
							},
						}))},
				},
			}),

			// End of types
		},
	}
}
