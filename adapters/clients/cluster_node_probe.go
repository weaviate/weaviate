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
)

// nodeProbe is the shared skeleton of the read-only cluster-internal probes:
// resolve a node name to a host, GET a JSON route, decode the answer.
type nodeProbe struct {
	client   *http.Client
	resolver nodeResolver
}

// getJSON GETs path on nodeName and decodes the response body into out.
//
// A 404 returns notFound unwrapped, so a caller can tell "this build does not
// serve the route" (rolling upgrade) apart from a transport failure. Every
// probe's admission decision turns on that distinction.
//
// what names the route in each error, e.g. "node activity".
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

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read %s response: %w", what, err)
	}

	if res.StatusCode == http.StatusNotFound {
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
