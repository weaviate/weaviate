package helper

import (
	"encoding/json"
	"testing"
)

// Asserts that the request did not return an error.
func AssertRequestOk(t *testing.T, response interface{}, err error, check_fn func()) {
	if err != nil {
		response_json, _ := json.MarshalIndent(response, "", "  ")
		t.Fatalf("Failed to perform request, because %#v. Response:\n%s", err, response_json)
	} else {
		if check_fn != nil {
			check_fn()
		}
	}
}
