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

// Pins: the reindex gate probe transport must never honor HTTP_PROXY, or a
// proxy answering with a byte-identical 404 could admit a reindex over a live
// backup. Asserted on the transport, not the built client, since the tracing
// wrapper in front of it exposes no way back to its base.
func TestReindexGateProbeTransportIgnoresProxyEnv(t *testing.T) {
	probe := clusterHttpTransport(time.Second, nil)
	require.Nil(t, probe.Proxy,
		"the gate's probes must reach the peer itself, never an HTTP_PROXY that can answer in its stead")

	// The shared client keeps its pre-existing proxy behavior.
	shared := clusterHttpTransport(time.Second, http.ProxyFromEnvironment)
	require.NotNil(t, shared.Proxy,
		"the shared cluster client's proxy behavior is pre-existing and must not change here")
}

// Drives the real client rather than asserting on the transport builder,
// since that takes the resolver as an argument and says nothing about which
// resolver the probe constructor chose. Both clients are pointed at an
// unresolvable peer and the connection error names whichever host was dialled.
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

	// Control: the shared client still routes through the proxy.
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
