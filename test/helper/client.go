//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	apiclient "github.com/weaviate/weaviate/client"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// clientIdleConnTimeout is how long the helper clients keep an idle connection
// in their pool. It must stay below the server's idle timeout (CleanupTimeout in
// adapters/handlers/rest/server.go, 10s by default): a connection the server has
// already closed can otherwise be reused, and a non-idempotent request such as a
// POST on it fails with a bare EOF instead of being retried.
const clientIdleConnTimeout = 5 * time.Second

// httpTransport is shared by all helper clients so they share one connection
// pool, as they did with http.DefaultTransport, whose 90s idle timeout outlives
// the server's.
var httpTransport http.RoundTripper = newHTTPTransport()

func newHTTPTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.IdleConnTimeout = clientIdleConnTimeout
	return transport
}

// newRuntime returns a REST runtime for host that uses the helper clients'
// shared transport. If a test case is provided, and we want to dump HTTP
// traffic, it logs HTTP traffic to the test case.
func newRuntime(t *testing.T, host, scheme string) *httptransport.Runtime {
	transport := httptransport.New(host, "/v1", []string{scheme})
	transport.Transport = httpTransport

	if t != nil && DebugHTTP {
		transport.SetDebug(true)
		transport.SetLogger(&testLogger{t: t})
	}
	return transport
}

// Create a client that logs with t.Logf, if a *testing.T is provided.
// If there is no test case at hand, pass in nil to disable logging.
func Client(t *testing.T) *apiclient.Weaviate {
	target, scheme := serverTarget()
	return apiclient.New(newRuntime(t, target, scheme), strfmt.Default)
}

// Create a auth writer for the Weaviate client with a specific key
func CreateAuth(apiKey string) runtime.ClientAuthInfoWriterFunc {
	return func(r runtime.ClientRequest, _ strfmt.Registry) error {
		return r.SetHeaderParam("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	}
}

func ClientGRPC(t *testing.T) pb.WeaviateClient {
	conn, err := CreateGrpcConnectionClient(GetWeaviateGRPCURL())
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)
	return grpcClient
}
