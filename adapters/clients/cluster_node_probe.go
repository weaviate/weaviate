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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/weaviate/weaviate/entities/clusterprobe"
)

// maxProbeResponseBytes bounds what a peer can make this node buffer. Every
// probe answer is a handful of JSON fields, so anything near this is not one.
const maxProbeResponseBytes = 64 << 10

// nodeNotFoundBody is the body http.NotFound sends, i.e. what a node's own
// catch-all handler answers for an unserved route.
const nodeNotFoundBody = "404 page not found"

// nodeProbe is the shared skeleton of the read-only cluster-internal probes:
// resolve a node name to a host, GET a JSON route, decode the answer.
//
// client must be appState.ClusterHttpClient: it is the one carrying the
// cluster credentials, so wherever basic auth is enabled on the internal API,
// any other client 401s and fails the caller's gate closed instead of
// reporting every node clear.
type nodeProbe struct {
	client   *http.Client
	resolver nodeResolver
}

// getJSON GETs path on nodeName and decodes the body into out; what names the
// route in errors, e.g. "node activity".
//
// Returns unanswerable, unwrapped, for a 404 shaped like a node's own
// catch-all (older build) or a 503 carrying the not-wired sentinel. Any other
// 404 is a plain error, since an intermediary answering in a node's stead
// would otherwise report every node as clear.
func (p nodeProbe) getJSON(ctx context.Context, nodeName, path string,
	query url.Values, unanswerable error, what string, out any,
) error {
	host, found := p.resolver.NodeHostname(nodeName)
	if !found {
		return fmt.Errorf("unable to resolve hostname for %q", nodeName)
	}

	u := url.URL{Scheme: "http", Host: host, Path: path, RawQuery: query.Encode()}
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
		return fmt.Errorf("read %s response: %w", what, err)
	}
	if len(body) > maxProbeResponseBytes {
		return fmt.Errorf("%s: response exceeds %d bytes", what, maxProbeResponseBytes)
	}

	if res.StatusCode == http.StatusNotFound {
		if !isNodeNotFound(res, body) {
			return fmt.Errorf("%s: 404 did not come from the node itself, so it does not "+
				"mean the route is unserved; check for an HTTP proxy on the cluster port (body: %s)",
				what, clusterprobe.Loggable(string(body)))
		}
		return unanswerable
	}
	if res.StatusCode == http.StatusServiceUnavailable && isProbeNotWired(res, body) {
		return unanswerable
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: unexpected status code %d (%s)",
			what, res.StatusCode, clusterprobe.Loggable(string(body)))
	}

	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unmarshal %s response: %w", what, err)
	}
	return nil
}

// isNodeNotFound reports whether a 404 has the shape of a node's own catch-all
// answer; see nodeNotFoundBody.
func isNodeNotFound(res *http.Response, body []byte) bool {
	return isNodeAnswer(res, body, nodeNotFoundBody)
}

// isProbeNotWired reports whether a 503 is a node saying it can never answer;
// see [clusterprobe.ProbeNotWiredMarker].
func isProbeNotWired(res *http.Response, body []byte) bool {
	return isNodeAnswer(res, body, clusterprobe.ProbeNotWiredMarker)
}

// isNodeAnswer reports whether body is want and carries the nosniff header
// http.Error sets, which an intermediary answering in a node's stead has no
// reason to send.
func isNodeAnswer(res *http.Response, body []byte, want string) bool {
	return res.Header.Get("X-Content-Type-Options") == "nosniff" &&
		strings.TrimSpace(string(body)) == want
}
