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
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

// RequestBaseURL returns the base URL a module request goes to: the value of
// baseURLHeader if the request sets it, else requested (a query-time baseURL),
// else configured (the class setting or module default). A base URL other than
// configured is used only if the request carries its own key in apiKeyHeader,
// so the server's key is only ever sent to the configured URL. The path counts
// too: a path on the configured host can reach another endpoint, or another
// tenant of a gateway. A value that differs from configured only in the case
// of the scheme or host, or in a trailing slash, returns configured.
func RequestBaseURL(ctx context.Context, baseURLHeader, requested, configured, apiKeyHeader string) (string, error) {
	source, baseURL := "a URL set in the query", requested
	if headerBaseURL := GetValueFromContext(ctx, baseURLHeader); headerBaseURL != "" {
		source, baseURL = baseURLHeader, headerBaseURL
	}
	if baseURL == "" || baseURL == configured || sameBaseURL(baseURL, configured) {
		return configured, nil
	}
	if err := RequireRequestAPIKey(ctx, source, apiKeyHeader); err != nil {
		return "", err
	}
	if err := ValidateBaseURL(baseURL); err != nil {
		return "", err
	}
	return baseURL, nil
}

// RequestAzureResourceName returns the Azure resource name a request goes to:
// the X-Azure-Resource-Name header if the request sets it, else requested,
// else configured. The resource name is the Azure host
// (<resourceName>.openai.azure.com), so the RequestBaseURL rule applies: a
// name other than configured needs X-Azure-Api-Key.
func RequestAzureResourceName(ctx context.Context, requested, configured string) (string, error) {
	source, name := "resourceName", requested
	if headerName := GetValueFromContext(ctx, "X-Azure-Resource-Name"); headerName != "" {
		source, name = "X-Azure-Resource-Name", headerName
	}
	if name == "" || strings.EqualFold(name, configured) {
		return configured, nil
	}
	if err := RequireRequestAPIKey(ctx, source, "X-Azure-Api-Key"); err != nil {
		return "", err
	}
	return name, nil
}

// RequireRequestAPIKey returns an ErrModulePrecondition unless the request
// carries a key in apiKeyHeader. source names the request value that changed
// the URL.
func RequireRequestAPIKey(ctx context.Context, source, apiKeyHeader string) error {
	if GetValueFromContext(ctx, apiKeyHeader) != "" {
		return nil
	}
	return enterrors.NewErrModulePrecondition(fmt.Errorf(
		"%s needs %s: the server's key is sent only to the configured URL", source, apiKeyHeader))
}

// sameBaseURL reports whether a and b differ only in the case of the scheme
// and host, or in a trailing slash. A value that does not parse never matches.
func sameBaseURL(a, b string) bool {
	ua, errA := url.Parse(a)
	ub, errB := url.Parse(b)
	if errA != nil || errB != nil || ua.Host == "" {
		return false
	}
	return strings.EqualFold(ua.Scheme, ub.Scheme) &&
		strings.EqualFold(ua.Host, ub.Host) &&
		ua.User.String() == ub.User.String() &&
		strings.TrimSuffix(ua.EscapedPath(), "/") == strings.TrimSuffix(ub.EscapedPath(), "/") &&
		ua.RawQuery == ub.RawQuery &&
		ua.EscapedFragment() == ub.EscapedFragment()
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

// ValidateBaseURL validates a URL a module sends requests to (class config, or
// a request URL that RequestBaseURL accepted) against SSRF abuse: https-only,
// non-empty host, and not pointing at
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
