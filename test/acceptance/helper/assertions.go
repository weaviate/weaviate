/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package helper

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Asserts that the request did not return an error.
// Optionally perform some checks only if the request did not fail
func AssertRequestOk(t *testing.T, response interface{}, err error, check_fn func()) {
	if err != nil {
		response_json, _ := json.MarshalIndent(response, "", "  ")
		errorPayload, _ := json.MarshalIndent(err, "", " ")
		t.Fatalf("Failed to perform request! Error: %s %s (Original error %s). Response: %s", getType(err), errorPayload, err, response_json)
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

// Get type name of some value, according to https://stackoverflow.com/questions/35790935/using-reflection-in-go-to-get-the-name-of-a-struct
func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

type fakeT struct {
	lastError error
}

func (f *fakeT) Reset() {
	f.lastError = nil
}

func (f *fakeT) Errorf(msg string, args ...interface{}) {
	f.lastError = fmt.Errorf(msg, args...)
}

// AssertEventuallyEqual retries the 'actual' thunk every 10ms for a total of
// 300ms. If a single one succeeds, it returns, if all fails it eventually
// fails
func AssertEventuallyEqual(t *testing.T, expected interface{}, actualThunk func() interface{}, msg ...interface{}) {
	interval := 10 * time.Millisecond
	timeout := 4000 * time.Millisecond
	elapsed := 0 * time.Millisecond
	fakeT := &fakeT{}

	for elapsed < timeout {
		fakeT.Reset()
		actual := actualThunk()
		assert.Equal(fakeT, expected, actual, msg...)

		if fakeT.lastError == nil {
			return
		}

		time.Sleep(interval)
		elapsed += interval
	}

	t.Errorf("waiting for %s, but never succeeded:\n\n%s", elapsed, fakeT.lastError)
}
