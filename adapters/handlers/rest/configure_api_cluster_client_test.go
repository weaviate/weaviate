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

package rest

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The gate's probes ask a named peer a question only that peer can answer, and
// read a 404 as "this build predates the route". An egress proxy standing in
// for the peer 404s everything, which reports every node as free of backups and
// fails the gate open cluster-wide.
//
// Asserted on the transport rather than on the built client because the tracing
// wrapper in front of it exposes no way back to its base, and adding one purely
// so a test can look would be a seam in production code.
func TestReindexGateProbeTransportIgnoresProxyEnv(t *testing.T) {
	probe := clusterHttpTransport(time.Second, nil)
	require.Nil(t, probe.Proxy,
		"the gate's probes must reach the peer itself, never an HTTP_PROXY that can answer in its stead")

	// Pins the scope of that decision: whether cluster-internal traffic at large
	// should honour a proxy is a deployment-visible question this PR does not
	// settle, so the shared client keeps the behavior it has always had.
	shared := clusterHttpTransport(time.Second, http.ProxyFromEnvironment)
	require.NotNil(t, shared.Proxy,
		"the shared cluster client's proxy behavior is pre-existing and must not change here")
}
