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

package helper

import (
	"fmt"
	"testing"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/batch"
	operations_apiclient "github.com/weaviate/weaviate/client/operations"
)

// Create a client that logs with t.Logf, if a *testing.T is provided.
// If there is no test case at hand, pass in nil to disable logging.
func OperationsClient(t *testing.T) operations_apiclient.ClientService {
	transport := httptransport.New(fmt.Sprintf("%s:%s", ServerHost, ServerPort), "/v1", []string{ServerScheme})

	// If a test case is provided, and we want to dump HTTP traffic,
	// create a simple logger that logs HTTP traffic to the test case.
	if t != nil && DebugHTTP {
		transport.SetDebug(true)
		transport.SetLogger(&testLogger{t: t})
	}

	client := operations_apiclient.New(transport, strfmt.Default)
	return client
}

// Create a client that logs with t.Logf, if a *testing.T is provided.
// If there is no test case at hand, pass in nil to disable logging.
func BatchClient(t *testing.T) batch.ClientService {
	transport := httptransport.New(fmt.Sprintf("%s:%s", ServerHost, ServerPort), "/v1", []string{ServerScheme})

	// If a test case is provided, and we want to dump HTTP traffic,
	// create a simple logger that logs HTTP traffic to the test case.
	if t != nil && DebugHTTP {
		transport.SetDebug(true)
		transport.SetLogger(&testLogger{t: t})
	}

	client := batch.New(transport, strfmt.Default)
	return client
}
