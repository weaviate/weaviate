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
)

// maxProbeResponseBytes bounds what a peer can make this node buffer. Every
// probe answer is a handful of JSON fields, so anything near this is not one.
const maxProbeResponseBytes = 64 << 10

// A node that does not serve a route falls through to the cluster API's
// catch-all handler, which calls http.NotFound. That gives a body and a
// nosniff header fixed by the standard library, and those two together are
// what a 404 has to carry before it counts as "this build is older".
const nodeNotFoundBody = "404 page not found"

// nodeProbe is the shared skeleton of the read-only cluster-internal probes:
// resolve a node name to a host, GET a JSON route, decode the answer.
type nodeProbe struct {
	client   *http.Client
	resolver nodeResolver
}

// getJSON GETs path on nodeName and decodes the body into out; what names the
// route in errors, e.g. "node activity".
//
// A 404 that carries the shape of a node's own catch-all answer returns
// notFound unwrapped, so callers can tell a build that does not serve the route
// (rolling upgrade) from a transport failure — every probe's admission decision
// turns on that. Any other 404 is an error instead: an intermediary answering
// in a node's stead (a transparent proxy, a misrouted ingress) that 404s
// everything would otherwise report every node in the cluster as clear.
func (p nodeProbe) getJSON(ctx context.Context, nodeName, path string,
	query url.Values, notFound error, what string, out any,
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
				what, snippet(body))
		}
		return notFound
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: unexpected status code %d (%s)", what, res.StatusCode, body)
	}

	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unmarshal %s response: %w", what, err)
	}
	return nil
}

// isNodeNotFound reports whether a 404 has the shape of a node's own catch-all
// answer; see nodeNotFoundBody.
func isNodeNotFound(res *http.Response, body []byte) bool {
	return res.Header.Get("X-Content-Type-Options") == "nosniff" &&
		strings.TrimSpace(string(body)) == nodeNotFoundBody
}

func snippet(body []byte) string {
	const max = 120
	if len(body) > max {
		return string(body[:max]) + "..."
	}
	return string(body)
}
