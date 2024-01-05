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

package helper

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Asserts that the request did not return an error.
// Optionally perform some checks only if the request did not fail
func AssertRequestOk(t *testing.T, response interface{}, err error, checkFn func()) {
	t.Helper()
	if err != nil {
		responseJson, _ := json.MarshalIndent(response, "", "  ")
		errorPayload, _ := json.MarshalIndent(err, "", " ")
		t.Fatalf("Failed to perform request! Error: %s %s (Original error %s). Response: %s", getType(err), errorPayload, err, responseJson)
	} else {
		if checkFn != nil {
			checkFn()
		}
	}
}

// Asserts that the request _did_ return an error.
// Optionally perform some checks only if the request failed
func AssertRequestFail(t *testing.T, response interface{}, err error, checkFn func()) {
	if err == nil {
		responseJson, _ := json.MarshalIndent(response, "", "  ")
		t.Fatalf("Request succeeded unexpectedly. Response:\n%s", responseJson)
	} else {
		if checkFn != nil {
			checkFn()
		}
	}
}

// Get type name of some value, according to https://stackoverflow.com/questions/35790935/using-reflection-in-go-to-get-the-name-of-a-struct
func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}
