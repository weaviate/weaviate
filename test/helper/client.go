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

// This file contains the Client(t *testing.T) function, that can be used to construct a client that talks to
// the Weaviate server that is configured using command line arguments (see init.go).
//
// We pass in the test (*testing.T), to be able to log HTTP traffic to that specific test case.
// This allows us to get detailed logs of the performed HTTP requests if a acceptance test fails.

// The CreateAuth returns a function that attaches the key and token headers to each HTTP call.

// Example:
//     func TestSomething(t *testing.T) {
//         // Use specific key & token
//         auth := helper.CreateAuth(key, token)
//         helper.Client(t).SomeScope.SomeOperation(&someParams, auth)
//
//         // Use root key & token
//         helper.Client(t).SomeScope.SomeOperation(&someParams, helper.RootAuth)
//     }

import (
	"fmt"
	"testing"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	apiclient "github.com/weaviate/weaviate/client"
)

// Create a client that logs with t.Logf, if a *testing.T is provided.
// If there is no test case at hand, pass in nil to disable logging.
func Client(t *testing.T) *apiclient.Weaviate {
	transport := httptransport.New(fmt.Sprintf("%s:%s", ServerHost, ServerPort), "/v1", []string{ServerScheme})

	// If a test case is provided, and we want to dump HTTP traffic,
	// create a simple logger that logs HTTP traffic to the test case.
	if t != nil && DebugHTTP {
		transport.SetDebug(true)
		transport.SetLogger(&testLogger{t: t})
	}

	client := apiclient.New(transport, strfmt.Default)
	return client
}

// Create a Weaviate client for the given API key & token.
func CreateAuth(apiKey strfmt.UUID, apiToken string) runtime.ClientAuthInfoWriterFunc {
	// Create an auth writer that both sets the api key & token.
	authWriter := func(r runtime.ClientRequest, _ strfmt.Registry) error {
		err := r.SetHeaderParam("X-API-KEY", string(apiKey))
		if err != nil {
			return err
		}

		return r.SetHeaderParam("X-API-TOKEN", apiToken)
	}

	return authWriter
}
