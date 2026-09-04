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

package protocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DefaultServerURL is the production license service.
const DefaultServerURL = "https://license.weaviate.io"

// Client performs signed verify calls against the license service. It is the
// piece Weaviate embeds; it depends only on the standard library.
type Client struct {
	ServerURL   string // defaults to DefaultServerURL
	LicenseID   string // from ParseKey
	PrivateKey  ed25519.PrivateKey
	TrustedKeys ServerKeySet // server public keys embedded in the binary
	HTTPClient  *http.Client // defaults to a 10s-timeout client
	UserAgent   string
}

// NewClient parses a customer license key and returns a client for it.
func NewClient(key string, trusted ServerKeySet) (*Client, error) {
	id, priv, err := ParseKey(key)
	if err != nil {
		return nil, err
	}
	return &Client{LicenseID: id, PrivateKey: priv, TrustedKeys: trusted}, nil
}

// Error is a structured error returned by the service.
type Error struct {
	HTTPStatus int
	Code       string `json:"error"`
	Message    string `json:"message"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("license: server returned %d %s: %s", e.HTTPStatus, e.Code, e.Message)
}

// Verify signs a challenge for this node and returns the server's answer.
// The response signature is checked against TrustedKeys and the nonce is
// checked against the request, so a non-nil result can be trusted.
func (c *Client) Verify(ctx context.Context, clusterID, instanceID, weaviateVersion string) (VerifyResponse, error) {
	req := VerifyRequest{
		LicenseID:       c.LicenseID,
		ClusterID:       clusterID,
		InstanceID:      instanceID,
		WeaviateVersion: weaviateVersion,
	}
	if err := req.Sign(c.PrivateKey); err != nil {
		return VerifyResponse{}, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return VerifyResponse{}, err
	}

	base := c.ServerURL
	if base == "" {
		base = DefaultServerURL
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/verify", bytes.NewReader(body))
	if err != nil {
		return VerifyResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if c.UserAgent != "" {
		httpReq.Header.Set("User-Agent", c.UserAgent)
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	res, err := hc.Do(httpReq)
	if err != nil {
		return VerifyResponse{}, fmt.Errorf("license: verify request: %w", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if err != nil {
		return VerifyResponse{}, fmt.Errorf("license: read response: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		e := &Error{HTTPStatus: res.StatusCode}
		if json.Unmarshal(raw, e) != nil || e.Code == "" {
			e.Code = "http_error"
			e.Message = http.StatusText(res.StatusCode)
		}
		return VerifyResponse{}, e
	}
	var resp VerifyResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return VerifyResponse{}, fmt.Errorf("license: decode response: %w", err)
	}
	if err := c.TrustedKeys.Verify(resp); err != nil {
		return VerifyResponse{}, err
	}
	if !resp.Matches(req) {
		return VerifyResponse{}, errors.New("license: response does not match request")
	}
	return resp, nil
}
