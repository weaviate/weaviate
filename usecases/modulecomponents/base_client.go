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

package modulecomponents

import (
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"
)

// NewBaseHttpClient creates an http.Client that does not follow redirects and
// resends a request whose pooled connection broke before a response arrived, so
// an origin may see the same request more than once. The timeout lives in the
// transport, not http.Client.Timeout, so replacing Transport also removes it.
// When MODULES_VALIDATE_BASE_URL is enabled it also installs a dial guard
// rejecting internal addresses — the network-layer SSRF backstop covering every
// client and baseURL source (config, headers, redirects).
func NewBaseHttpClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport
	if BaseURLValidationEnabled() {
		transport = guardDial(http.DefaultTransport.(*http.Transport).Clone())
	}
	return newBaseHttpClient(timeout, transport)
}

// NewBaseHttpClientWithIdleConns is NewBaseHttpClient on a transport of its own
// that keeps up to idlePerHost idle connections per host, and twice that over
// all hosts. The shared default keeps two per host, so a client that sends
// more requests at once to one host opens most of its connections, and TLS
// handshakes, anew for every batch of requests.
func NewBaseHttpClientWithIdleConns(timeout time.Duration, idlePerHost int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = idlePerHost
	transport.MaxIdleConns = 2 * idlePerHost
	if BaseURLValidationEnabled() {
		transport = guardDial(transport)
	}
	return newBaseHttpClient(timeout, transport)
}

func newBaseHttpClient(timeout time.Duration, transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: &retryTransport{base: transport, timeout: timeout},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// guardDial keeps DefaultTransport's dialer settings and adds a Control hook
// that runs after DNS resolution with the concrete IP about to be dialed.
func guardDial(transport *http.Transport) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   ssrfDialGuard,
	}
	transport.DialContext = dialer.DialContext
	return transport
}

// ssrfDialGuard blocks connections to internal addresses. address is the
// resolved "ip:port", so a host that resolves to an internal IP is caught here
// even if it slipped past ValidateBaseURL's string-level checks.
func ssrfDialGuard(network, address string, c syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid dial address %q: %w", address, err)
	}
	if ip := net.ParseIP(host); ip != nil && isDisallowedIP(ip) {
		return fmt.Errorf("refusing to dial internal address %q", address)
	}
	return nil
}
