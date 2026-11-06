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
)

// AllowedBaseURLsEnv holds comma-separated origins: scheme://host[:port].
const AllowedBaseURLsEnv = "MODULES_WEAVIATE_EMBEDDINGS_ALLOWED_BASE_URLS"

// ValidateBaseURL accepts only DefaultBaseURL and origins in AllowedBaseURLsEnv.
func ValidateBaseURL(baseURL string) error {
	_, err := resolveOrigin(baseURL)
	return err
}

// Callers must build the request URL from the returned origin, not from baseURL.
func resolveOrigin(baseURL string) (string, error) {
	if baseURL == "" {
		return DefaultBaseURL, nil
	}
	origin, ok := parseOrigin(baseURL)
	if !ok || (origin != DefaultBaseURL && !slices.Contains(allowedOrigins(), origin)) {
		return "", fmt.Errorf("baseURL %q is not allowed", baseURL)
	}
	return origin, nil
}

func allowedOrigins() []string {
	var origins []string
	for _, entry := range strings.Split(os.Getenv(AllowedBaseURLsEnv), ",") {
		if origin, ok := parseOrigin(strings.TrimSpace(entry)); ok {
			origins = append(origins, origin)
		}
	}
	return origins
}

func parseOrigin(rawURL string) (string, bool) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return "", false
	}
	return (&url.URL{Scheme: parsed.Scheme, Host: strings.ToLower(parsed.Host)}).String(), true
}
