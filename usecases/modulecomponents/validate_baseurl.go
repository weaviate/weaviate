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

// ValidatedBaseURLFromHeader returns the baseURL carried in the given request
// header if present and valid, otherwise fallback. Validation is a no-op unless
// MODULES_VALIDATE_BASE_URL is enabled. It folds the recurring
// fetch-check-validate-assign block in module URL builders into one call.
func ValidatedBaseURLFromHeader(ctx context.Context, headerKey, fallback string) (string, error) {
	headerBaseURL := GetValueFromContext(ctx, headerKey)
	if headerBaseURL == "" {
		return fallback, nil
	}
	if err := ValidateBaseURL(headerBaseURL); err != nil {
		return "", err
	}
	return headerBaseURL, nil
}

// RequestBaseURL returns the base URL a module request goes to: the value of
// baseURLHeader if the request sets it, else requested (a query-time baseURL),
// else configured (the class setting or module default). A base URL on another
// scheme or host than configured is used only if the request carries its own
// key in one of apiKeyHeaders, so the server's key is never sent to a host the
// request names. fromRequest reports that case: the caller must then send
// nothing else from server configuration, such as an organization ID.
func RequestBaseURL(ctx context.Context, baseURLHeader, requested, configured string, apiKeyHeaders ...string,
) (baseURL string, fromRequest bool, err error) {
	source, baseURL := "baseURL", requested
	if headerBaseURL := GetValueFromContext(ctx, baseURLHeader); headerBaseURL != "" {
		source, baseURL = baseURLHeader, headerBaseURL
	}
	if baseURL == "" {
		return configured, false, nil
	}
	if baseURL == configured || sameOrigin(baseURL, configured) {
		return baseURL, false, nil
	}
	if err := RequireRequestAPIKey(ctx, source, apiKeyHeaders...); err != nil {
		return "", false, err
	}
	if err := ValidateBaseURL(baseURL); err != nil {
		return "", false, err
	}
	return baseURL, true, nil
}

// RequireRequestAPIKey returns an error unless the request carries a key in
// one of apiKeyHeaders. source names the request value that set the host.
func RequireRequestAPIKey(ctx context.Context, source string, apiKeyHeaders ...string) error {
	for _, header := range apiKeyHeaders {
		if GetValueFromContext(ctx, header) != "" {
			return nil
		}
	}
	return fmt.Errorf("%s needs %s: the server's key is not sent to a host named by the request",
		source, strings.Join(apiKeyHeaders, " or "))
}

// sameOrigin reports whether a and b parse to the same scheme and host:port.
// A value that does not parse is never the same origin.
func sameOrigin(a, b string) bool {
	ua, errA := url.Parse(a)
	ub, errB := url.Parse(b)
	if errA != nil || errB != nil || ua.Host == "" {
		return false
	}
	return strings.EqualFold(ua.Scheme, ub.Scheme) && strings.EqualFold(ua.Host, ub.Host)
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
