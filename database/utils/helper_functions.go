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

package connutils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"

	"github.com/go-openapi/strfmt"
	gouuid "github.com/satori/go.uuid"

	"github.com/creativesoftwarefdn/weaviate/usecases/config"
)

// NowUnix returns the current Unix time
func NowUnix() int64 {
	return MakeUnixMillisecond(time.Now())
}

// MakeUnixMillisecond returns the millisecond unix-version of the given time
func MakeUnixMillisecond(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

// GenerateUUID returns a new UUID
func GenerateUUID() strfmt.UUID {

	// generate the uuid
	uuid, err := gouuid.NewV4()

	// panic, can't create uuid
	if err != nil {
		panic("PANIC: Can't create UUID")
	}

	// return the uuid and the error
	return strfmt.UUID(fmt.Sprintf("%v", uuid))
}

// Must panics if error, otherwise returns value
func Must(i interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return i
}

// WhereStringToStruct is the 'compiler' for converting the filter/where query-string into a struct
func WhereStringToStruct(prop string, where string) (WhereQuery, error) {
	whereQuery := WhereQuery{}

	// Make a regex which can compile a string like 'firstName>=~John'
	re1, _ := regexp.Compile(`^([a-zA-Z0-9]*)([:<>!=]*)([~]*)([^~]*)$`)
	result := re1.FindStringSubmatch(where)

	// Set which property
	whereQuery.Property = prop
	if len(result[1]) > 1 && len(result[4]) != 0 {
		whereQuery.Property = fmt.Sprintf("%s.%s", prop, result[1])
	}

	// Set the operator
	switch result[2] {
	// When operator is "", put in 'Equal' as operator
	case ":", "", "=":
		whereQuery.Value.Operator = Equal
	case "!:", "!=":
		whereQuery.Value.Operator = NotEqual
	// TODO: https://github.com/creativesoftwarefdn/weaviate/issues/202
	// case ">":
	// 	whereQuery.Value.Operator = GreaterThan
	// case ">:", ">=":
	// 	whereQuery.Value.Operator = GreaterThanEqual
	// case "<":
	// 	whereQuery.Value.Operator = LessThan
	// case "<:", "<=":
	// 	whereQuery.Value.Operator = LessThanEqual
	default:
		return whereQuery, errors.New("invalid operator set in query")
	}

	// The wild cards
	// TODO: Wildcard search is disabled for now https://github.com/creativesoftwarefdn/weaviate/issues/202
	whereQuery.Value.Contains = false //result[3] == "~"

	// Set the value itself
	if len(result[4]) == 0 {
		if len(result[1]) > 0 && len(result[2]) == 0 && len(result[3]) == 0 {
			// If only result[1] is set, just use that as search term.
			whereQuery.Value.Value = result[1]
		} else {
			// When value is "", throw error
			return whereQuery, errors.New("no value is set in the query")
		}
	} else {
		whereQuery.Value.Value = result[4]
	}

	return whereQuery, nil
}

// DoExternalRequest does a request to an external Weaviate Instance based on given parameters
func DoExternalRequest(instance config.Instance, endpoint string, uuid strfmt.UUID) (response *http.Response, err error) {
	// Create the transport and HTTP client
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}}

	// Create the request with basic headers
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/weaviate/v1/%s/%s", instance.URL, endpoint, uuid), nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-API-KEY", instance.APIKey)
	req.Header.Set("X-API-TOKEN", instance.APIToken)

	// Do the request
	response, err = client.Do(req)

	if err != nil {
		return
	}

	// Check the status-code to determine existence
	if response.StatusCode != 200 {
		err = fmt.Errorf("status code is not 200, but %d with status '%s'", response.StatusCode, response.Status)
	}

	return
}

// ResolveExternalCrossRef resolves an object on an external instance using the given parameters and the Weaviate REST-API of the external instance
func ResolveExternalCrossRef(instance config.Instance, endpoint string, uuid strfmt.UUID, responseObject interface{}) (err error) {
	// Do the request
	response, err := DoExternalRequest(instance, endpoint, uuid)

	// Return error
	if err != nil {
		return
	}

	// Close the body on the end of the function
	defer response.Body.Close()

	// Read the body and fill the object with the data from the response
	body, _ := ioutil.ReadAll(response.Body)
	json.Unmarshal(body, responseObject)

	return
}
