//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package rest

import (
	"reflect"
	"testing"
)

func TestCreateErrorResponseObject(t *testing.T) {
	testResults := createErrorResponseObject("error message 1", "error message 2")

	// check which type is used
	if typeName := reflect.TypeOf(testResults); typeName.Kind() == reflect.Ptr {
		if typeName.Elem().Name() != "ErrorResponse" {
			t.Error("Wrong struct used, should be ErrorResponse but is: ", typeName.Elem().Name())
		}
	} else {
		t.Error("Wrong struct used, should be ErrorResponse but is: ", typeName.Name())
	}

}
