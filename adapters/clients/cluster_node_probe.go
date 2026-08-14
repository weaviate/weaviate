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

package clients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/clusterprobe"
	"github.com/weaviate/weaviate/usecases/cluster"
)

const maxProbeResponseBytes = 64 << 10

// http.Client reads a zero Timeout as unbounded, so a configured non-positive
// budget must never reach it.
const defaultProbeTimeout = 30 * time.Second

// ErrProbeUnauthorized never means a node is free: a node whose credentials we
// fail is a node we cannot read.
var ErrProbeUnauthorized = errors.New("the peer refused the cluster credentials")

type nodeProbe struct {
	client   *http.Client
	resolver nodeResolver
}

// probeHTTPClient must not be unified with the shared cluster client: that one
// honors HTTP_PROXY/HTTPS_PROXY and follows redirects, either of which lets a
// host we never asked answer for a peer, and its 404 clears that peer.
func probeHTTPClient(authConfig cluster.AuthConfig, probeTimeout time.Duration) *http.Client {
	if probeTimeout <= 0 {
		probeTimeout = defaultProbeTimeout
	}
	var transport http.RoundTripper = &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: probeTimeout, KeepAlive: 120 * time.Second}).DialContext,
		MaxIdleConnsPerHost:   4,
		MaxIdleConns:          16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   probeTimeout,
		ExpectContinueTimeout: time.Second,
	}
	if authConfig.BasicAuth.Enabled() {
		transport = basicAuthTransport{next: transport, auth: authConfig.BasicAuth}
	}
	return &http.Client{
		Transport: transport,
		Timeout:   probeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type basicAuthTransport struct {
	next http.RoundTripper
	auth cluster.BasicAuth
}

func (t basicAuthTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Mutating the caller's request would carry the credential onto a redirect
	// the stdlib strips it from.
	clone := r.Clone(r.Context())
	clone.SetBasicAuth(t.auth.Username, t.auth.Password)
	return t.next.RoundTrip(clone)
}

// getJSON returns unanswerable only for a 404 the node itself wrote.
func (p nodeProbe) getJSON(ctx context.Context, nodeName, path string,
	unanswerable error, what string, out any,
) error {
	host, found := p.resolver.NodeHostname(nodeName)
	if !found {
		return fmt.Errorf("%s: cannot resolve hostname for node %s", what, clusterprobe.Loggable(nodeName))
	}

	u := url.URL{Scheme: "http", Host: host, Path: path}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("new %s request: %w", what, err)
	}

	res, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s request: %w", what, err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(io.LimitReader(res.Body, maxProbeResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read %s answer: %w", what, err)
	}
	if len(body) > maxProbeResponseBytes {
		return fmt.Errorf("%s: answer exceeds %d bytes", what, maxProbeResponseBytes)
	}

	switch {
	case res.StatusCode >= 300 && res.StatusCode < 400:
		return fmt.Errorf("%s: status %d redirecting to %s, which the probe does not follow, "+
			"because an answer from a host we did not address cannot say whether this node is "+
			"free", what, res.StatusCode, clusterprobe.Loggable(res.Header.Get("Location")))
	case res.StatusCode == http.StatusNotFound && isNodeNotFound(res, body):
		return unanswerable
	case res.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%s: a 404 that did not come from the node itself does not mean the "+
			"route is unserved, so it cannot mean the node is free; check for an HTTP proxy on "+
			"the cluster port (body: %s)", what, clusterprobe.Loggable(string(body)))
	case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
		return fmt.Errorf("%s: status %d, %w; check that CLUSTER_BASIC_AUTH_USERNAME and "+
			"CLUSTER_BASIC_AUTH_PASSWORD are the same on every node", what, res.StatusCode,
			ErrProbeUnauthorized)
	case res.StatusCode != http.StatusOK:
		return fmt.Errorf("%s: status %d (%s)", what, res.StatusCode, clusterprobe.Loggable(string(body)))
	}

	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unmarshal %s answer: %w", what, err)
	}
	return nil
}

func isNodeNotFound(res *http.Response, body []byte) bool {
	// The exact body is what discriminates; nosniff is only a cheap extra
	// necessary condition, since proxies set it as a blanket header too.
	return res.Header.Get(clusterprobe.NodeNotFoundHeader) == clusterprobe.NodeNotFoundHeaderValue &&
		strings.TrimSpace(string(body)) == clusterprobe.NodeNotFoundBody
}
