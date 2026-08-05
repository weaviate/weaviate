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
	"github.com/weaviate/weaviate/usecases/cluster"
)

// The gate's probes ask a named peer a question only that peer can answer, and
// read a 404 as "this build predates the route". The 404-shape check rejects an
// ordinary proxy error page, which leaves the measured hole this closes: a proxy
// answering these routes with a 404 byte-identical to Go's stdlib one. Against
// that, a reindex submission is admitted over a live backup while the operator
// is told a rolling upgrade is in progress.
//
// Fail-without receipt: /tmp/qa-pr12474/rvcd/logs/case1e-stdlib404.log
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

// Status alone is not the oracle, and neither is the transport builder: the
// builder takes the resolver as an argument, so asserting on it says nothing
// about which resolver the probe CONSTRUCTOR chose. This drives the real client.
//
// A live server cannot decide it either — Go bypasses proxies for loopback, so
// an httptest peer is reached directly whichever client is used. Instead both
// clients are pointed at an unresolvable cluster-shaped peer, and the connection
// error names whichever host was actually dialled.
func TestReindexGateProbeClientDialsThePeerNotTheProxy(t *testing.T) {
	const (
		peerURL   = "http://weaviate-2.invalid:7101/backups/node-activity"
		proxyHost = "egress.corp.invalid"
	)
	t.Setenv("HTTP_PROXY", "http://"+proxyHost+":3128")

	probeErr := dialErr(t, reindexGateProbeHttpClient(cluster.AuthConfig{}, time.Second), peerURL)
	require.NotContains(t, probeErr, proxyHost,
		"the gate's probe must dial the peer, never a proxy that would answer in its stead")
	require.Contains(t, probeErr, "weaviate-2.invalid",
		"the probe must have tried the peer itself")

	// The other half of the scope decision: the shared client is untouched and
	// still routes through the proxy. If this fails, the environment was already
	// resolved before the test set it, and the assertion above proved nothing.
	sharedErr := dialErr(t, reasonableHttpClient(cluster.AuthConfig{}, time.Second), peerURL)
	require.Contains(t, sharedErr, proxyHost,
		"pre-existing behavior of the shared cluster client must be unchanged")
}

func dialErr(t *testing.T, c *http.Client, url string) string {
	t.Helper()
	resp, err := c.Get(url)
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected the request to fail against an unresolvable host")
	}
	return err.Error()
}
