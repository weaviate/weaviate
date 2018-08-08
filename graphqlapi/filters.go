package graphqlapi

import (
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"reflect"
	//"regexp"
)

// struct inputs in struct argumentconfig.Type in struct fieldconfigargument (map string:*argumentconfig) in struct field.Args

// WORKS, but an EnumConfig's values are the end of the line; eg. no way to access the Value property of testObjectvalue.
func genFilterFields() (*graphql.Enum, error) {
	outputEnum := graphql.NewEnum(graphql.EnumConfig{
		Name:        "WeaviateLocalConvertedFetchFilterInpObj",
		Description: "Filter options for the converted fetch search, to convert the data to the filter input",
		Values: graphql.EnumValueConfigMap{
			"AND": &graphql.EnumValueConfig{
				Value:       "FetchFilterInputAndObj",
				Description: "Filter options for the converted fetch search, to convert the data to the filter input",
			},
			"enumAsValueToAchieveMoreLayers": &graphql.EnumValueConfig{
				Value: *graphql.NewEnum(graphql.EnumConfig{
					Name:        "testEnumLayer",
					Description: "stuff",
					Values: graphql.EnumValueConfigMap{
						"AND": &graphql.EnumValueConfig{
							Value:       "w/e",
							Description: "more stuff",
						},
					},
				}),
				Description: "filter where the path end should be inequal to the value",
			},
			"testObjectvalue": &graphql.EnumValueConfig{
				Value: *graphql.NewEnum(graphql.EnumConfig{
					Name:        "testEnumLayer",
					Description: "stuff",
					Values: graphql.EnumValueConfigMap{
						"AND": &graphql.EnumValueConfig{
							Value:       "w/e",
							Description: "more stuff",
						},
					},
				}),
				Description: "filter where the path end should be inequal to the value",
			},
		},
	})
	return outputEnum, nil
}

//// Attempt using custom versions of the required structs
//// DOES NOT WORK; custom structs aren't recognised by gql because they aren't defined in the gql lib
//func genFilterFields() (*FilterEnum, error) {
//	outputEnum := FilterNewEnum(FilterEnumConfig{
//		Name:        "WeaviateLocalConvertedFetchFilterInpObj",
//		Description: "Filter options for the converted fetch search, to convert the data to the filter input",
//		Values: FilterEnumValueConfigMap{
//			"AND": &FilterEnumValueConfig{
//				Value:       "FetchFilterInputAndObj",
//				Description: "Filter options for the converted fetch search, to convert the data to the filter input",
//			},
//			"enumAsValueToAchieveMoreLayers": &FilterEnumValueConfig{
//				Value: *FilterNewEnum(FilterEnumConfig{
//					Name:        "testEnumLayer",
//					Description: "stuff",
//					Values: FilterEnumValueConfigMap{
//						"AND": &FilterEnumValueConfig{
//							Value:       "w/e",
//							Description: "more stuff",
//						},
//					},
//				}),
//				Description: "filter where the path end should be inequal to the value",
//			},
//		},
//	})
//	return outputEnum, nil
//}

// use an object
// Error: Introspection must provide input type for arguments.
//func genFilterFields() (*graphql.Object, error) {
//	outputEnum := graphql.NewObject(graphql.ObjectConfig{
//		Name:        "WeaviateLocalConvertedFetchFilterInpObj",
//		Description: "Filter options for the converted fetch search, to convert the data to the filter input",
//		Fields: graphql.Fields{
//			"AND": &graphql.Field{
//				Name:        "1",
//				Type:        graphql.String,
//				Description: "Filter options for the converted fetch search, to convert the data to the filter input",
//			},
//			"OR": &graphql.Field{
//				Name: "2",
//
//				Type:        graphql.String,
//				Description: "Filter options for the converted fetch search, to convert the data to the filter input",
//			},
//		},
//	})
//	return outputEnum, nil
//}

// So we use InputObjects, as these are registered as Input types in the GQL lib
// Error: GraphQL schema initialization gave an error when initializing: Could not build GraphQL schema, because:
// WeaviateLocalConvertedFetchFilterInpObj fields must be an object with field names as keys or a function which return such an object..
/*func genFilterFields() (*graphql.InputObject, error) {
	outputEnum := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:        "WeaviateLocalConvertedFetchFilterInpObj",
		Description: "Filter options for the converted fetch search, to convert the data to the filter input",
		Fields: &graphql.Fields{
			"AND": &graphql.Field{
				Name:        "1",
				Type:        graphql.String,
				Description: "Filter options for the converted fetch search, to convert the data to the filter input",
			},
			"OR": &graphql.Field{
				Name: "2",

				Type:        graphql.String,
				Description: "Filter options for the converted fetch search, to convert the data to the filter input",
			},
		},
	})
	return outputEnum, nil
}*/

// Let's try again, this time using all inputobject classes in the GQL lib
// ERROR: GraphQL schema initialization gave an error when initializing: Could not build GraphQL schema, because:
// WeaviateLocalConvertedFetchFilterInpObj fields must be an object with field names as keys or a function which return such an object..
//func genFilterFields() (*graphql.InputObject, error) {
//	outputEnum := graphql.NewInputObject(graphql.InputObjectConfig{
//		Name:        "WeaviateLocalConvertedFetchFilterInpObj",
//		Description: "Filter options for the converted fetch search, to convert the data to the filter input",
//		Fields: &graphql.InputObjectFieldMap{
//			"AND": &graphql.InputObjectField{
//				PrivateName:        "1",
//				Type:               graphql.String,
//				PrivateDescription: "Filter options for the converted fetch search, to convert the data to the filter input",
//			},
//			"OR": &graphql.InputObjectField{
//				PrivateName: "2",
//
//				Type:               graphql.String,
//				PrivateDescription: "Filter options for the converted fetch search, to convert the data to the filter input",
//			},
//		},
//	})
//	return outputEnum, nil
//}

////// attempt to use differing types of Fields iterable object, using an inputobject or w/e
////Fields: map[interface{}]*graphql.InputObject{ ::: ERROR: GraphQL schema initialization gave an error when initializing: Could not build GraphQL schema, because:
////        WeaviateLocalConvertedFetchFilterInpObj fields must be an object with field names as keys or a function which return such an object..
//
//// declaring testRegularArray := [1]*graphql.InputObject{<object>}:
//// ERROR: GraphQL schema initialization gave an error when initializing: Could not build GraphQL schema, because:
//// WeaviateLocalConvertedFetchFilterInpObj fields must be an object with field names as keys or a function which return such an object..
//func genFilterFields() (*graphql.InputObject, error) {
//
//	testRegularArray := [1]*graphql.InputObject{
//
//		graphql.NewInputObject(graphql.InputObjectConfig{
//
//			Name:        "test1",
//			Description: "desc1",
//			Fields: &graphql.Fields{
//
//				"OR": &graphql.Field{
//					Name: "2",
//
//					Type:        graphql.String,
//					Description: "Filter options for the converted fetch search, to convert the data to the filter input",
//				},
//			},
//		}),
//	}
//
//	outputEnum := graphql.NewInputObject(graphql.InputObjectConfig{
//		Name:        "WeaviateLocalConvertedFetchFilterInpObj",
//		Description: "Filter options for the converted fetch search, to convert the data to the filter input",
//		Fields:      testRegularArray,
//	})
//	return outputEnum, nil
//}

// Laura's definition of the fields the _filter should have
/*
var filterFields = {

  AND: {

    name: "FetchFilterAND",

    description: function() {

      return getDesc("FetchFilterAND")},

    type: new GraphQLInputObjectType({

      name: "FetchFilterANDInpObj",

      description: function() {

        return getDesc("FetchFilterANDInpObj")},

      fields: function () {return filterFields}

    })

  },

  OR: {

    name: "FetchFilterAND",

    description: function() {

      return getDesc("FetchFilterAND")},

    type: new GraphQLInputObjectType({

      name: "FetchFilterORInpObj",

      description: function() {

        return getDesc("FetchFilterORInpObj")},

      fields: function () {return filterFields}

    })

  },

  EQ: {

    name: 'FetchFilterEQ',

    description: function() {

      //console.log(Object.keys(this))

      return getDesc("FetchFilterEQ")},

    type: new GraphQLList(new GraphQLInputObjectType({ // is path equal to

      name: 'FetchFilterEQInpObj',

      description: function() {

        return getDesc(this.name)},

      fields: fetchFilterFields

    }))

  },

  NEQ: {

    name: 'FetchFilterNEQ',

    description: function() {

      return getDesc("FetchFilterNEQ")},

    type: new GraphQLList(new GraphQLInputObjectType({ // path is NOT equal to

      name: 'FetchFilterNEQInpObj',

      description: function() {

        return getDesc("FetchFilterNEQInpObj")},

      fields: fetchFilterFields

    }))

  },

  IE: {

    name: 'FetchFilterIE',

    description: function() {

      return getDesc("FetchFilterIE")},

    type: new GraphQLList(new GraphQLInputObjectType({ //  = InEquality between values.

      name: 'FetchFilterIEInpObj',

      description: function() {

        return getDesc("FetchFilterIEInpObj")},

      fields: fetchFilterFields

    }))

  }

}
*/

// Attempt to define and use custom Enum struct. Goal: to make the EnumValueConfigs able to correctly
// contain and use nested objects. As it is now their Value property is unusable for my purposes

type FilterEnum struct {
	PrivateName string `json:"name"`

	PrivateDescription string `json:"description"`

	enumConfig FilterEnumConfig

	values []*FilterEnumValueDefinition

	valuesLookup map[interface{}]*FilterEnumValueDefinition

	nameLookup map[string]*FilterEnumValueDefinition

	err error
}

type FilterEnumValueConfigMap map[string]*FilterEnumValueConfig

type FilterEnumValueConfig struct {
	Value interface{} `json:"value"`

	DeprecationReason string `json:"deprecationReason"`

	Description string `json:"description"`
}

type FilterEnumConfig struct {
	Name string `json:"name"`

	Values FilterEnumValueConfigMap `json:"values"`

	Description string `json:"description"`
}

type FilterEnumValueDefinition struct {
	Name string `json:"name"`

	Value interface{} `json:"value"`

	DeprecationReason string `json:"deprecationReason"`

	Description string `json:"description"`
}

func FilterNewEnum(config FilterEnumConfig) *FilterEnum {

	gt := &FilterEnum{}

	gt.enumConfig = config

	if gt.err = assertValidName(config.Name); gt.err != nil {

		return gt

	}

	gt.PrivateName = config.Name

	gt.PrivateDescription = config.Description

	if gt.values, gt.err = gt.defineEnumValues(config.Values); gt.err != nil {

		return gt

	}

	return gt

}

func (gt *FilterEnum) defineEnumValues(valueMap FilterEnumValueConfigMap) ([]*FilterEnumValueDefinition, error) {

	var err error

	values := []*FilterEnumValueDefinition{}

	//	if err = invariantf(
	//
	//		len(valueMap) > 0,
	//
	//		`%v values must be an object with value names as keys.`, gt,
	//	)*/; err != nil {

	return values, err

	//	}

	for valueName, valueConfig := range valueMap { // TODO

		//		if err = invariantf(
		//
		//			valueConfig != nil,
		//
		//			`%v.%v must refer to an object with a "value" key `+
		//
		//				`representing an internal value but got: %v.`, gt, valueName, valueConfig,
		//		); err != nil {

		return values, err

		//		}

		if err = assertValidName(valueName); err != nil {

			return values, err

		}

		value := &FilterEnumValueDefinition{

			Name: valueName,

			Value: valueConfig.Value,

			DeprecationReason: valueConfig.DeprecationReason,

			Description: valueConfig.Description,
		}

		if value.Value == nil {

			value.Value = valueName

		}

		values = append(values, value)

	}

	return values, nil

}

func (gt *FilterEnum) Values() []*FilterEnumValueDefinition {

	return gt.values

}

func (gt *FilterEnum) Serialize(value interface{}) interface{} {

	v := value

	rv := reflect.ValueOf(v)

	if kind := rv.Kind(); kind == reflect.Ptr && rv.IsNil() {

		return nil

	} else if kind == reflect.Ptr {

		v = reflect.Indirect(reflect.ValueOf(v)).Interface()

	}

	if enumValue, ok := gt.getValueLookup()[v]; ok { // TODO

		return enumValue.Name

	}

	return nil

}

func (gt *FilterEnum) ParseValue(value interface{}) interface{} {

	var v string

	switch value := value.(type) {

	case string:

		v = value

	case *string:

		v = *value

	default:

		return nil

	}

	if enumValue, ok := gt.getNameLookup()[v]; ok {

		return enumValue.Value

	}

	return nil

}

func (gt *FilterEnum) ParseLiteral(valueAST ast.Value) interface{} {

	if valueAST, ok := valueAST.(*ast.EnumValue); ok {

		if enumValue, ok := gt.getNameLookup()[valueAST.Value]; ok {

			return enumValue.Value

		}

	}

	return nil

}

func (gt *FilterEnum) Name() string {

	return gt.PrivateName

}

func (gt *FilterEnum) Description() string {

	return gt.PrivateDescription

}

func (gt *FilterEnum) String() string {

	return gt.PrivateName

}

func (gt *FilterEnum) Error() error {

	return gt.err

}

func (gt *FilterEnum) getValueLookup() map[interface{}]*FilterEnumValueDefinition {

	if len(gt.valuesLookup) > 0 {

		return gt.valuesLookup

	}

	valuesLookup := map[interface{}]*FilterEnumValueDefinition{}

	for _, value := range gt.Values() {

		valuesLookup[value.Value] = value

	}

	gt.valuesLookup = valuesLookup

	return gt.valuesLookup

}

func (gt *FilterEnum) getNameLookup() map[string]*FilterEnumValueDefinition {

	if len(gt.nameLookup) > 0 {

		return gt.nameLookup

	}

	nameLookup := map[string]*FilterEnumValueDefinition{}

	for _, value := range gt.Values() {

		nameLookup[value.Name] = value

	}

	gt.nameLookup = nameLookup

	return gt.nameLookup

}

//var NameRegExp = regexp.MustCompile("^[_a-zA-Z][_a-zA-Z0-9]*$")

func assertValidName(name string) error {

	return fmt.Errorf(`Names must match /^[_a-zA-Z][_a-zA-Z0-9]*$/ but "%v" does not.`, name)
	/*invariantf(

	NameRegExp.MatchString(name),

	`Names must match /^[_a-zA-Z][_a-zA-Z0-9]*$/ but "%v" does not.`, name)*/

}
