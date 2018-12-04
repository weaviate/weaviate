package helper

// This file contains the Client(t *testing.T) function, that can be used to construct a client that talks to
// the Weaviate server that is configured using command line arguments (see init.go).
//
// We pass in the test (*testing.T), to be able to log HTTP traffic to that specific test case.
// This allows us to get detailed logs of the performaned HTTP requests if a acceptance test fails.

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

	//operations_apiclient "github.com/creativesoftwarefdn/weaviate/client/operations"
	apiclient "github.com/creativesoftwarefdn/weaviate/client"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
)

// Create a client that logs with t.Logf, if a *testing.T is provided.
// If there is no test case at hand, pass in nil to disable logging.
func OperationsClient(t *testing.T) *apiclient.WeaviateDecentralisedKnowledgeGraph {
	transport := httptransport.New(fmt.Sprintf("%s:%s", ServerHost, ServerPort), "/weaviate/v1", []string{ServerScheme})

	// If a test case is provided, and we want to dump HTTP trafic,
	// create a simple logger that logs HTTP trafic to the test case.
	if t != nil && DebugHTTP {
		transport.SetDebug(true)
		transport.SetLogger(&testLogger{t: t})
	}

	client := /*operations_*/apiclient.New(transport, strfmt.Default)
	return client
}
