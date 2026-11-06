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

package weaviateembed

import (
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
)

// AllowedBaseURLsEnv holds comma-separated origins: scheme://host[:port].
const AllowedBaseURLsEnv = "MODULES_WEAVIATE_EMBEDDINGS_ALLOWED_BASE_URLS"

// ValidateBaseURL accepts only DefaultBaseURL and origins in AllowedBaseURLsEnv.
func ValidateBaseURL(baseURL string) error {
	allowedOrigins, _ := parseAllowlist()
	_, err := resolveOrigin(baseURL, allowedOrigins)
	return err
}

// WarnInvalidAllowedBaseURLs logs allowlist entries that can never match. Rejected
// baseURLs get a generic error, so the log is the only place these show up.
func WarnInvalidAllowedBaseURLs(logger logrus.FieldLogger) {
	_, invalidEntries := parseAllowlist()
	for _, entry := range invalidEntries {
		logger.WithField("env", AllowedBaseURLsEnv).
			Warnf("ignoring entry %q: expected scheme://host[:port] without wildcards", entry)
	}
}

// Callers must build the request URL from the returned origin, not from baseURL.
func resolveOrigin(baseURL string, allowedOrigins []string) (string, error) {
	if baseURL == "" {
		return DefaultBaseURL, nil
	}
	origin, ok := parseOrigin(baseURL)
	if !ok || (origin != DefaultBaseURL && !slices.Contains(allowedOrigins, origin)) {
		return "", fmt.Errorf("baseURL %q is not allowed", baseURL)
	}
	return origin, nil
}

func parseAllowlist() (origins, invalidEntries []string) {
	for _, rawEntry := range strings.Split(os.Getenv(AllowedBaseURLsEnv), ",") {
		entry := strings.TrimSpace(rawEntry)
		if entry == "" {
			continue
		}
		origin, ok := parseOrigin(entry)
		if !ok || strings.Contains(origin, "*") {
			invalidEntries = append(invalidEntries, entry)
			continue
		}
		origins = append(origins, origin)
	}
	return origins, invalidEntries
}

func parseOrigin(rawURL string) (string, bool) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", false
	}
	return (&url.URL{Scheme: parsed.Scheme, Host: lowercaseHost(parsed.Host)}).String(), true
}

// Host names are case-insensitive, but IPv6 zone identifiers are interface names.
func lowercaseHost(host string) string {
	zoneStart := strings.IndexByte(host, '%')
	if zoneStart < 0 {
		return strings.ToLower(host)
	}
	return strings.ToLower(host[:zoneStart]) + host[zoneStart:]
}
