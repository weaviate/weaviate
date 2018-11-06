package helper

import (
	"encoding/json"
	"testing"
)

// Asserts that the request did not return an error.
// Optionally perform some checks only if the request did not fail
func AssertRequestOk(t *testing.T, response interface{}, err error, check_fn func()) {
	if err != nil {
		response_json, _ := json.MarshalIndent(response, "", "  ")
		errorPayload, _ := json.MarshalIndent(err, "", " ")
		t.Fatalf("Failed to perform request, because %s. Response:\n%s", errorPayload, response_json)
	} else {
		if check_fn != nil {
			check_fn()
		}
	}
}

// Asserts that the request _did_ return an error.
// Optionally perform some checks only if the request failed
func AssertRequestFail(t *testing.T, response interface{}, err error, check_fn func()) {
	if err == nil {
		response_json, _ := json.MarshalIndent(response, "", "  ")
		t.Fatalf("Request succeeded unexpectedly. Response:\n%s", response_json)
	} else {
		if check_fn != nil {
			check_fn()
		}
	}
}
