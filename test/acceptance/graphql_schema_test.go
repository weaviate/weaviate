package test

// Acceptance tests for GraphQL Schema

// There is a helper struct called GraphQLResult that helps to navigate through the output,
// and a few helper functions to access the GraphQL endpoint.
// See the end of this file for more details on how those work.

import (
	"testing"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/go-openapi/runtime"

	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/stretchr/testify/assert"

	graphql_client "github.com/creativesoftwarefdn/weaviate/client/graphql"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
)

// Test that the types we expect there to be are actually generated.
func TestGettingTypeNames(t *testing.T) {
	t.Skip()
	t.Parallel()

	query := `{ 
    __schema { 
      types {
        name
      }
    }
  }`

	expectedTypes := []string{"WeaviateObj", "WeaviateLocalObj", "WeaviateLocalConvertedFetchFilterInpObj", "FetchFilterANDInpObj", "FetchFilterORInpObj", "FetchFilterEQInpObj", "FetchFilterFieldANDInpObj", "FetchFilterFieldORInpObj", "FetchFilterNEQInpObj", "FetchFilterIEInpObj", "WeaviateLocalConvertedFetchObj", "WeaviateLocalConvertedFetchThingsObj", "WeaviateLocalConvertedFetchActionsObj", "WeaviateLocalMetaFetchFilterInpObj", "WeaviateLocalMetaFetchObj", "WeaviateLocalMetaFetchGenericsObj", "WeaviateLocalMetaFetchGenericsThingsObj", "WeaviateLocalMetaFetchGenericsActionsObj"}

	response := queryGraphqlAssertOK(t, helper.RootAuth, query)

	for _, expected := range expectedTypes {
		found := false

		// Check if this expected type is in the response
		for _, type_ := range response.AssertKey(t, "__schema").AssertKey(t, "types").AssertSlice(t) {
			foundType := type_.AssertKey(t, "name").AssertString(t)
			if expected == foundType {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Could not find expected type '%s'", expected)
		}
	}
}

// Checks if root query type is as expected
func TestRootType(t *testing.T) {
	t.Skip()
	t.Parallel()

	query := `{
		__schema {
		  queryType {
			name
		  }
		}
	  }`

	response := queryGraphqlAssertOK(t, helper.RootAuth, query)

	assert.Equal(t, response.AssertKey(t, "__schema").AssertKey(t, "queryType").AssertKey(t, "name"), "WeaviateObj", "Root type should be the same as expected.")
}

// Checks if root query type is correctly defined
func TestRootQueryType(t *testing.T) {
	t.Skip()
	t.Parallel()

	response := queryGraphqlTypeIntrospection(t, helper.RootAuth, "WeaviateObj")

	assert.Equal(t, response.AssertKey(t, "__type").AssertKey(t, "kind"), "OBJECT", "Root query type object kind should be same as expected.")
	assert.Equal(t, response.AssertKey(t, "__type").AssertKey(t, "name"), "WeaviateObj", "Root query type name should be same as expected.")
	assert.Equal(t, response.AssertKey(t, "__type").AssertKey(t, "description"), "Location of the root query", "Root query type description should be same as expected.")
}

// Checks schema types are generated as expected
func TestSchemaTypes(t *testing.T) {
	t.Skip()
	t.Parallel()

	// Read entire grapql schema descriptions file into an array of bytes
	expected_schema_file, err := ioutil.ReadFile("../graphql_schema/schema_design.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed read file: %s\n", err)
		os.Exit(1)
	}
	var f interface{}
	err = json.Unmarshal(expected_schema_file, &f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s\n", err)
		os.Exit(1)
	}

	// Use the same helper functions to navigate through the fixture as we use to navigate through responses from Weaviate.
	expected_schema := &GraphQLResult{Result: f}

	// loop through all expected types
	for _, expected_type := range expected_schema.AssertKey(t, "data").AssertKey(t, "__schema").AssertKey(t, "types").AssertSlice(t) {
		// introspect the type in the actual schema
		response := queryGraphqlTypeIntrospection(t, helper.RootAuth, expected_type.AssertKey(t, "name").AssertString(t))

		// assert the actual type
		assert.Equal(t, response.AssertKey(t, "__type").AssertKey(t, "kind"), expected_type.AssertKey(t, "kind"), "The object's kind type should be the same as expected.")
		assert.Equal(t, response.AssertKey(t, "__type").AssertKey(t, "description"), expected_type.AssertKey(t, "description"), "The object's description should be the same as expected.")

		var fields string
		_ = fields
		if response.AssertKey(t, "__type").AssertKey(t, "kind").AssertString(t) == "OBJECT" {
			fields = "fields"
		} else if response.AssertKey(t, "__type").AssertKey(t, "kind").AssertString(t) == "INPUT_OBJECT" {
			fields = "inputFields"
		} else if response.AssertKey(t, "__type").AssertKey(t, "kind").AssertString(t) == "ENUM" {
			// assert the enumValues
			for _, expectedEnumValue := range expected_type.AssertKey(t, "enumValues").AssertSlice(t) {
				//found := false
				for _, actualEnumValue := range response.AssertKey(t, "__type").AssertKey(t, "kind").AssertKey(t, "enumValues").AssertSlice(t) {
					if expectedEnumValue.AssertKey(t, "name").AssertString(t) == actualEnumValue.AssertKey(t, "name").AssertString(t) {
						//found = true
						// assert description
						assert.Equal(t, actualEnumValue.AssertKey(t, "description").AssertString(t), expectedEnumValue.AssertKey(t, "description").AssertString(t), "The enum description should be the same as expected.")
						break
					}
				}
			}
		} else if response.AssertKey(t, "__type").AssertKey(t, "kind").AssertString(t) == "UNION" {
			//			// assert type
			//			for _, expected_possible_type := range expected_type.AssertKey(t, fields).AssertSlice(t) {
			//				found := false
			//				for _, actual_possible_type := range response.AssertKey(t, "__type").AssertKey(t, "fields").AssertSlice(t) {
			//					if expected_possible_type.AssertKey(t, "name").AssertString(t) == actual_possible_type.AssertKey(t, "name").AssertString(t) {
			//						found = true
			//						// possible type is found
			//
			//						// assert field type
			//						hasName := actual_possible_type.HasKey(t,"name")
			//						hasOfType := actual_possible_type.HasKey(t, "ofType")
			//						if hasName {
			//							assert.Equal(t, name, expected_possible_type.AssertKey(t, "type").AssertKey(t, "name").AssertString(t), "The field type's name should be the same as expected.")
			//						}
			//						if ofType {
			//							for true {
			//								assert.Equal(t, actual_possible_type(t, "ofType").AssertKey(t, "name").AssertString(t), expected_possible_type.AssertKey(t, "ofType").AssertKey(t, "name").AssertString(t), "The field type's name should be the same as expected.")
			//								actual_possible_type := actual_possible_type.AssertKey(t, "ofType").AssertKey(t)
			//								name := actual_possible_type.AssertKey(t, "name").AssertKey(t)
			//								if name != nil {
			//									break
			//								}
			//							}
			//						}
			//						break
			//					}
			//				}
			//			}
		}

		//		if fields {
		//			for _, expected_field := range expected_type.AssertKey(t, fields).AssertSlice(t) {
		//				found := false
		//				for _, actual_field := range response.AssertKey(t, "__type").AssertKey(t, "fields").AssertSlice(t) {
		//					if expected_field.AssertKey(t, "name").AssertString(t) == actual_field.AssertKey(t, "name").AssertString(t) {
		//						// field is found
		//						found = true
		//
		//						// assert field description
		//						assert.Equal(t, actual_field.AssertKey(t, "description").AssertString(t), expected_field.AssertKey(t, "description").AssertString(t), "The fields description should be the same as expected.")
		//
		//						// assert field type
		//						actual_field_type := actual_field.AssertKey(t, "type").AssertString(t)
		//						name := actual_field_type.AssertKey(t, "name").AssertString(t)
		//						ofType := actual_field_type.AssertKey(t, "ofType").AssertString(t)
		//						if name {
		//							assert.Equal(t, name, expected_field.AssertKey(t, "type").AssertKey(t, "name").AsserString(t), "The field type's name should be the same as expected.")
		//						}
		//						if ofType {
		//							for true {
		//								assert.Equal(t, actual_field_type(t, "ofType").AssertKey(t, "name").AssertString(t), expected_field.AssertKey(t, "ofType").AssertKey(t, "name").AssertString(t), "The field type's name should be the same as expected.")
		//								actual_field_type := actual_field_type.AssertKey(t, "ofType").AssertString(t)
		//								name := actual_field_type.AssertKey(t, "name").AssertString(t)
		//								if name != nil {
		//									break
		//								}
		//							}
		//						}
		//
		//						// assert field args
		//						for _, expected_arg := range expected_field.AssertKey(t, "args").AssertSlice(t) {
		//							found := false
		//							for _, actual_arg := range actual_field.AssertKey(t, "args").AssertSlice(t) {
		//								if expected_arg.AssertKey(t, "name").AssertString(t)  == actual_arg.AssertKey(t, "name").AssertString(t) {
		//									found = true
		//
		//									// assert arg description
		//									assert.Equal(t, actual_arg.AssertKey(t, "description").AssertString(t) , expected_arg.AssertKey(t, "description").AssertString(t) , "the arg's description should be the same as expected.")
		//
		//									// assert arg type
		//									actual_arg_type := actual_arg.AssertKey(t, "type").AssertString(t)
		//									name := actual_arg_type.AssertKey(t, "name").AssertString(t)
		//									ofType := actual_arg_type.AssertKey(t, "ofType").AssertString(t)
		//									if name {
		//										assert.Equal(t, name, expected_arg.AssertKey(t, "type").AssertKey(t, "name").AssertString(t) , "The field type's name should be the same as expected.")
		//									}
		//									if ofType {
		//										for true {
		//											assert.Equal(t, actual_arg_type(t, "ofType").AssertKey(t, "name").AssertString(t) , expected_arg.AssertKey(t, "ofType").AssertKey(t, "name").AssertString(t) , "The field type's name should be the same as expected.")
		//											actual_arg_type := actual_arg_type.AssertKey(t, "ofType").AssertString(t)
		//											name := actual_arg_type.AssertKey(t, "name").AssertString(t)
		//											if name != nil {
		//												break
		//											}
		//										}
		//									}
		//
		//									break
		//								}
		//							}
		//						}
		//
		//
		//						break
		//					}
		//				}
		//			}
		//		}
	}
}

// TO DO
// Test naming convention camel case
// func TestSchemaNamingConvention(t *testing.T) {
// 	t.Parallel()

///////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions

type GraphQLResult struct {
	Result interface{}
}

// Asserts that the result is a map, and index in this key
func (g *GraphQLResult) AssertKey(t *testing.T, key string) *GraphQLResult {
	m, ok := g.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("Can't index into key %s, because this is not a map", key)
	}

	x, ok := m[key]
	if !ok {
		t.Fatalf("Can't index into key %s, because no such key exists", key)
	}

	return &GraphQLResult{Result: x}
}

// TODO Use this function instead of AssertKey(t, "x").AssetKey(t, "y"):
//  AssertKeys(t, []string{"x","y",})
func (g *GraphQLResult) AsserKeyPath(t *testing.T, keys []string) *GraphQLResult {
	// currently found result.
	r := g.Result

	for _, key := range keys {
		m, ok := r.(map[string]interface{})
		if !ok {
			t.Fatalf("Can't index into key %s, because this is not a map", key)
		}

		r, ok = m[key]
		if !ok {
			t.Fatalf("Can't index into key %s, because no such key exists", key)
		}
	}

	return &GraphQLResult{Result: r}
}

// Assert that this is a slice.
// Wraps a GraphQLResult over all children too.
func (g *GraphQLResult) AssertSlice(t *testing.T) []*GraphQLResult {
	m, ok := g.Result.([]interface{})
	if !ok {
		t.Fatalf("This is not a slice!")
	}

	var result []*GraphQLResult

	for _, s := range m {
		result = append(result, &GraphQLResult{Result: s})
	}

	return result
}

// Assert that this is a string
func (g *GraphQLResult) AssertString(t *testing.T) string {
	str, ok := g.Result.(string)
	if !ok {
		t.Fatalf("This is not a string!")
	}
	return str
}

// Perform a GraphQL query
func queryGraphql(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}) (*models.GraphQLResponse, error) {
	var vars interface{} = variables
	params := graphql_client.NewWeaviateGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: operation, Query: query, Variables: vars})
	response, err := helper.Client(t).Graphql.WeaviateGraphqlPost(params, auth)

	if err != nil {
		return nil, err
	}

	return response.Payload, nil
}

// Perform a query and assert that it is successful
func queryGraphqlAssertOK(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, query string) *GraphQLResult {
	response, err := queryGraphql(t, auth, "", query, nil)
	if err != nil {
		t.Fatalf("Expected the query to succeed, but failed due to: %#v", err)
	}

	data := make(map[string]interface{})

	// get rid of models.JSONData
	for key, value := range response.Data {
		data[key] = value
	}

	return &GraphQLResult{Result: data}
}

// Perform a query and assert that it is incorrect
func queryGraphqlAssertFail(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, query string) ([]*models.GraphQLError, error) {
	response, err := queryGraphql(t, auth, "", query, nil)
	if err == nil {
		t.Fatalf("Expected the query to fail, but it succeeded %#v", response)
	}

	return response.Errors, err
}

// Perform a GraphQL introspection query for a specific type
// TODO add assert OK in name
func queryGraphqlTypeIntrospection(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, typeName string) *GraphQLResult {
	query := fmt.Sprintf(`{
		__type(name: %s) {
		  kind
		  name
		  description
		  fields {
			name
			description
			args {
			  ...InputValue
			}
			type {
			  ...TypeRef
			}
		  }
		  inputFields {	
			...InputValue
		  }
		  interfaces {
			...TypeRef
		  }
		  enumValues {
			name
			description
		  }
		  possibleTypes {
			...TypeRef
		  }
		}
	  }
	  
	  fragment InputValue on __InputValue {
		name
		description
		type {
		  ...TypeRef
		}
	  }
	  
	  fragment TypeRef on __Type {
		kind
		name
		ofType {
		  kind
		  name
		  ofType {
			kind
			name
			ofType {
			  kind
			  name
			  ofType {
				kind
				name
				ofType {
				  kind
				  name
				  ofType {
					kind
					name
					ofType {
					  kind
					  name
					}
				  }
				}
			  }
			}
		  }
		}
	  }`, typeName)

	response := queryGraphqlAssertOK(t, helper.RootAuth, query)
	return response
}
