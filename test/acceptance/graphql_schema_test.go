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
package test

// Acceptance tests for GraphQL Schema

// There is a helper struct called GraphQLResult that helps to navigate through the output,
// and a few helper functions to access the GraphQL endpoint.
// See the end of this file for more details on how those work.

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	graphql_client "github.com/creativesoftwarefdn/weaviate/client/graphql"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
)

func init() {
	skipGraphqlTest = (os.Getenv("GRAPHQL_TESTS") == "skip")
}

var skipGraphqlTest bool

func maybeSkipGraphqlTest(t *testing.T) {
	if skipGraphqlTest {
		t.Skip("Skipping GraphQL tests")
	}
}

/* Skip length comparisons of field lengths due to multiple schema's being loaded into Weaviate when running the acceptance test,
causing the Weaviate's schema to be larger than the prototype's schema*/
var fieldLengthComparisonEnabled bool = false

/*
Loop through all branches of the expected schema and compare each leaf to an
actual schema retrieved from an introspection query on a local weaviate

Note: Can't compare nested lists ([[][]]) properly, but that is out of scope for the current case
*/
func TestCompareExpectedToActualSchemaWithIntrospection(t *testing.T) {
	maybeSkipGraphqlTest(t)
	t.Parallel()

	// get expected schema
	rawFile, _ := ioutil.ReadFile("../graphql_schema/schema_design.json")
	var data map[string]interface{}
	err := json.Unmarshal(rawFile, &data)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("data successfully read")
	}
	expectedSchema := data["data"].(map[string]interface{})

	// get actual schema
	query := genQuery()
	response := queryGraphqlAssertOK(t, helper.RootAuth, query)
	actualSchema := response.Result.(map[string]interface{})

	//compare expected schema to actual schema, element by element
	for expectedTopLayerKey, expectedTopLayerValue := range expectedSchema {
		actualTopLayerValue := actualSchema[expectedTopLayerKey].(map[string]interface{})
		traverseNestedSchemaLayer(t, expectedTopLayerKey, expectedTopLayerValue.(map[string]interface{}), actualTopLayerValue)
	}
}

func traverseNestedSchemaLayer(t *testing.T, expectedLayerKey string, expectedLayerValue map[string]interface{}, actualLayerValue map[string]interface{}) {
	// attempt to add the name of the current element to the path
	updatedExpectedLayerKey := expectedLayerKey
	if expectedLayerName, ok := expectedLayerValue["name"]; ok {
		updatedExpectedLayerKey = fmt.Sprintf("%s_%s", updatedExpectedLayerKey, expectedLayerName)
	}
	/* Directives and Subscriptions are part of the core library. There are a handful of differences in element descriptions
	between the Go and JS implementations that raise errors in this test, but these are not of interest to us (see issue 390
	in graphql-go for an example).*/
	if !strings.Contains(updatedExpectedLayerKey, "irective") && !strings.Contains(updatedExpectedLayerKey, "ubscription") {

		for key, expectedValue := range expectedLayerValue {
			schemaPath := fmt.Sprintf("%s_%s", updatedExpectedLayerKey, key)

			// Don't compare description texts
			if !strings.Contains(schemaPath, "description") {

				if actualValue, ok := actualLayerValue[key]; ok {
					compareExpectedElementToActualElement(t, schemaPath, expectedValue, actualValue)
				} else {
					t.Errorf(fmt.Sprintf("Element %s not found in map at path %s", key, schemaPath))
				}
			}
		}
	}
}

func compareExpectedElementToActualElement(t *testing.T, schemaPath string, expectedValue interface{}, actualValue interface{}) {
	switch expectedValue.(type) {

	case []interface{}:
		parsedExpectedValue := expectedValue.([]interface{})
		// check if both items are lists
		assert.Equal(t, reflect.TypeOf(expectedValue), reflect.TypeOf(actualValue), fmt.Sprintf("Array type inequality detected at path: %s", schemaPath))
		if reflect.TypeOf(expectedValue) == reflect.TypeOf(actualValue) {
			// check if both lists have the same length
			parsedActualValue := actualValue.([]interface{})
			if fieldLengthComparisonEnabled {
				assert.Equal(t, len(parsedExpectedValue), len(parsedActualValue), fmt.Sprintf("Array length inequality detected at path: %s", schemaPath))
			}
			if len(parsedExpectedValue) > 0 {
				handleListComparisons(t, schemaPath, parsedExpectedValue, parsedActualValue)
			}
		}

	case map[string]interface{}:
		// check if both values are maps of the same type
		assert.Equal(t, reflect.TypeOf(expectedValue), reflect.TypeOf(actualValue), fmt.Sprintf("Map type inequality detected at path: %s", schemaPath))
		parsedExpectedValue := expectedValue.(map[string]interface{})
		if reflect.TypeOf(expectedValue) == reflect.TypeOf(actualValue) {
			parsedActualValue := actualValue.(map[string]interface{})

			if fieldLengthComparisonEnabled {
				// check if both maps have the same length
				assert.Equal(t, len(parsedExpectedValue), len(parsedActualValue), fmt.Sprintf("Map length inequality detected at path: %s", schemaPath))
			}

			if len(parsedExpectedValue) > 0 {
				traverseNestedSchemaLayer(t, schemaPath, parsedExpectedValue, parsedActualValue)
			}
		}

	case nil:

	default:
		/* There are a handful of differences in element descriptions between the Go and JS implementations that raise errors in
		this test, but these are not of interest to us (see issue 390 in graphql-go for an example).*/
		if schemaPath != "__schema_types___TypeKind_description" {
			assert.Equal(t, expectedValue, actualValue, fmt.Sprintf("Scalar value inequality detected at path: %s", schemaPath))
		}
	}
}

func handleListComparisons(t *testing.T, schemaPath string, expectedList []interface{}, actualList []interface{}) {
	// determine the type of the contents of this list
	_, elementIsMap := expectedList[0].(map[string]interface{})

	// determine if the actual list contains the elements found in the expected list
	for _, expectedElement := range expectedList {
		if elementIsMap {
			fetchExpectedMapElementFromActualList(t, schemaPath, expectedElement, actualList)
		}
		if !elementIsMap {
			fetchExpectedScalarElementFromActualList(t, schemaPath, expectedElement, actualList)
		}
	}
}

func fetchExpectedMapElementFromActualList(t *testing.T, schemaPath string, expectedElement interface{}, actualList []interface{}) {
	// get the current element's name
	parsedExpectedElement := expectedElement.(map[string]interface{})
	expectedName := parsedExpectedElement["name"]

	// attempt to find a matching element in the actual list
	expectedElementFoundInActualList := false
	for _, actualElement := range actualList {
		parsedActualElement := actualElement.(map[string]interface{})
		actualName := parsedActualElement["name"]

		if actualName == expectedName {
			expectedElementFoundInActualList = true
			traverseNestedSchemaLayer(t, schemaPath, parsedExpectedElement, parsedActualElement)
		}
	}
	assert.Equal(t, true, expectedElementFoundInActualList, fmt.Sprintf("Expected element %s not found in path %s", expectedName, schemaPath))
}

func fetchExpectedScalarElementFromActualList(t *testing.T, schemaPath string, expectedElement interface{}, actualList []interface{}) {
	expectedElementFoundInActualList := false
	for _, actualElement := range actualList {
		if actualElement == expectedElement {
			expectedElementFoundInActualList = true
		}
	}
	assert.Equal(t, true, expectedElementFoundInActualList, fmt.Sprintf("Expected element %s not found in path %s", expectedElement, schemaPath))
}

func genQuery() string {
	return `query IntrospectionQuery {

    __schema {

      queryType { name }

      mutationType { name }

      subscriptionType { name }

      types {

        ...FullType

      }

      directives {

        name

        description

        args {

          ...InputValue

        }

        onOperation

        onFragment

        onField

      }

    }

  }



  fragment FullType on __Type {

    kind

    name

    description

    fields(includeDeprecated: true) {

      name

      description

      args {

        ...InputValue

      }

      type {

        ...TypeRef

      }

      isDeprecated

      deprecationReason

    }

    inputFields {

      ...InputValue

    }

    interfaces {

      ...TypeRef

    }

    enumValues(includeDeprecated: true) {

      name

      description

      isDeprecated

      deprecationReason

    }

    possibleTypes {

      ...TypeRef

    }

  }



  fragment InputValue on __InputValue {

    name

    description

    type { ...TypeRef }

    defaultValue

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

        }

      }

    }

  }`
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions

type GraphQLResult struct {
	Result interface{}
}

// Perform a GraphQL query
func queryGraphql(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}) (*models.GraphQLResponse, error) {
	var vars interface{} = variables
	params := graphql_client.NewWeaviateGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: operation, Query: query, Variables: vars})
	response, err := helper.Client(t).Graphql.WeaviateGraphqlPost(params, nil)

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
