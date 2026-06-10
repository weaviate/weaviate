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
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	entcfg "github.com/weaviate/weaviate/entities/config"
)

// Hostnames and suffixes that are always rejected, independent of DNS.
var (
	blockedHostnames    = []string{"localhost"}
	blockedHostSuffixes = []string{".local", ".internal", ".localdomain"}
)

// BaseURLValidationEnabled reports whether SSRF hardening of module baseURLs is
// on. Gated behind MODULES_VALIDATE_BASE_URL so operators that legitimately
// target HTTP proxies or private-network gateways are not broken by default.
func BaseURLValidationEnabled() bool {
	return entcfg.Enabled(os.Getenv("MODULES_VALIDATE_BASE_URL"))
}

// ValidateBaseURLHeader validates the baseURL carried in the given request
// header (e.g. "X-Openai-Baseurl"). It is the call-site guard for module
// clients; a no-op when the header is absent or validation is disabled.
func ValidateBaseURLHeader(ctx context.Context, headerKey string) error {
	return ValidateBaseURL(GetValueFromContext(ctx, headerKey))
}

// isDisallowedIP reports whether an IP points at the local host or an internal
// network. Used both by ValidateBaseURL and by the dial guard, where it sees
// the actually-resolved IP and so also defeats DNS rebinding (TOCTOU).
func isDisallowedIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified()
}

// ValidateBaseURL validates a module baseURL (class config or X-*-Baseurl
// header) against SSRF abuse: https-only, non-empty host, and not pointing at
// an internal address (by IP literal, by blocked hostname/suffix, or by DNS
// resolution). It is a no-op unless MODULES_VALIDATE_BASE_URL is enabled; an
// empty baseURL means "use the module default" and is allowed.
func ValidateBaseURL(baseURL string) error {
	if !BaseURLValidationEnabled() || baseURL == "" {
		return nil
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("invalid baseURL: %w", err)
	}
	if parsed.Scheme != "https" {
		return fmt.Errorf("baseURL must use HTTPS")
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("baseURL must have a non-empty host")
	}

	if ip := net.ParseIP(host); ip != nil {
		if isDisallowedIP(ip) {
			return fmt.Errorf("baseURL cannot target internal addresses")
		}
		return nil
	}

	lower := strings.ToLower(host)
	for _, blocked := range blockedHostnames {
		if lower == blocked {
			return fmt.Errorf("baseURL cannot target internal addresses")
		}
	}
	for _, suffix := range blockedHostSuffixes {
		if strings.HasSuffix(lower, suffix) {
			return fmt.Errorf("baseURL cannot target internal addresses")
		}
	}

	// Resolve and reject if any resolved IP is internal; a resolvable public
	// host is required. Short timeout so validation can't block.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	addrs, err := (&net.Resolver{}).LookupHost(ctx, host)
	if err != nil {
		return fmt.Errorf("baseURL host could not be resolved: %w", err)
	}
	for _, addr := range addrs {
		if ip := net.ParseIP(addr); ip != nil && isDisallowedIP(ip) {
			return fmt.Errorf("baseURL cannot target internal addresses")
		}
	}

	return nil
}
