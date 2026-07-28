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
// resends a request whose connection broke before a response arrived, so an
// origin may see the same request more than once. The timeout lives in the
// transport, not http.Client.Timeout, so replacing Transport also removes it.
// When MODULES_VALIDATE_BASE_URL is enabled it also installs a dial guard
// rejecting internal addresses — the network-layer SSRF backstop covering every
// client and baseURL source (config, headers, redirects).
func NewBaseHttpClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport

	if BaseURLValidationEnabled() {
		// DefaultTransport's dialer settings plus a Control hook that runs
		// after DNS resolution with the concrete IP about to be dialed.
		dialer := &net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			Control:   ssrfDialGuard,
		}
		guarded := http.DefaultTransport.(*http.Transport).Clone()
		guarded.DialContext = dialer.DialContext
		transport = guarded
	}

	return &http.Client{
		Transport: &retryTransport{base: transport, timeout: timeout},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
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
