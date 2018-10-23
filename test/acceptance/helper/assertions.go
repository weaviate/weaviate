package helper

import (
	"encoding/json"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Asserts that the request did not return an error.
// Optionally perform some checks only if the request did not fail
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

func AssertOneErrorMessage(t *testing.T, expectedMessage string, errors *models.ErrorResponse) {
	errorLen := len(errors.Error)
	assert.Equal(t, 1, errorLen)

	if errorLen > 0 {
		assert.Equal(t, expectedMessage, errors.Error[0].Message)
	}
}
