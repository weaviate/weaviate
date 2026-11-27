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
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
)

// AllowedBaseURLsEnv holds comma-separated origins (scheme://host[:port]) that a
// collection's module settings may point a module at, in addition to the
// module's own defaults. A module attaches the server's provider key to every
// request it sends, so only the operator may choose these hosts.
const AllowedBaseURLsEnv = "MODULES_ALLOWED_BASE_URLS"

// hostSettingNames are module setting names that choose, or take part in
// choosing, the host a module sends its requests to.
var hostSettingNames = []string{
	"baseURL", "apiEndpoint", "endpoint", "endpointURL", "endpointId",
	"inferenceUrl", "passageInferenceUrl", "queryInferenceUrl", "imageInferenceUrl",
	"resourceName", "region", "location", "projectId", "host", "url",
}

// HostSettingNames returns the module setting names that choose, or take part
// in choosing, the host a module sends its requests to. Compare them without
// regard to case.
func HostSettingNames() []string {
	return slices.Clone(hostSettingNames)
}

// HasHostSetting reports whether settings, or any object or list nested in
// them, has a key that HostSettingNames names, in any case.
func HasHostSetting(settings any) bool {
	switch v := settings.(type) {
	case map[string]any:
		for key, value := range v {
			if slices.ContainsFunc(hostSettingNames, func(name string) bool { return strings.EqualFold(name, key) }) {
				return true
			}
			if HasHostSetting(value) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(v, HasHostSetting)
	}
	return false
}

// Matched without case folding, so a non-ASCII letter that lowercases to an
// ASCII one (such as the Kelvin sign) is not accepted.
var dnsLabelPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

// ValidateCollectionBaseURL rejects a base URL set in a collection's module
// settings unless its origin is the origin of one of the module's defaults or
// is listed in AllowedBaseURLsEnv. A module attaches the server's provider key
// to its requests, so every module validation calls it, before anything that
// sends a request, for each setting that chooses the request host, and passes
// that setting's name for the error. An empty baseURL means "use the module
// default" and is accepted. A URL that does not parse to an origin, or whose
// host is not ASCII, is rejected. List such hosts in their punycode form.
func ValidateCollectionBaseURL(setting, baseURL string, defaults ...string) error {
	if baseURL == "" {
		return nil
	}
	origin, ok := BaseURLOrigin(baseURL)
	if !ok || !hasASCIIHost(baseURL) {
		return fmt.Errorf("%s %q is not a valid scheme://host[:port] URL", setting, baseURL)
	}
	allowed, _ := ParseAllowedOrigins(os.Getenv(AllowedBaseURLsEnv))
	want := withoutDefaultPort(origin)
	for _, candidate := range defaults {
		if defaultOrigin, ok := BaseURLOrigin(candidate); ok && withoutDefaultPort(defaultOrigin) == want {
			return nil
		}
	}
	for _, candidate := range allowed {
		if withoutDefaultPort(candidate) == want {
			return nil
		}
	}
	return fmt.Errorf("%s %q is not allowed: add %q to %s to let collections use it",
		setting, baseURL, want, AllowedBaseURLsEnv)
}

// withoutDefaultPort drops :443 from an https origin and :80 from an http one,
// so that both spellings of the same origin compare equal.
func withoutDefaultPort(origin string) string {
	switch {
	case strings.HasPrefix(origin, "https://"):
		return strings.TrimSuffix(origin, ":443")
	case strings.HasPrefix(origin, "http://"):
		return strings.TrimSuffix(origin, ":80")
	default:
		return origin
	}
}

// ValidateAzureResourceName rejects an Azure OpenAI resource name that is not a
// single DNS label. The name becomes the request host
// (<resourceName>.openai.azure.com), so "attacker.example/x?" would move the
// request, and the Azure key, to attacker.example. A single label stays under
// openai.azure.com, a domain the provider owns.
func ValidateAzureResourceName(resourceName string) error {
	if resourceName == "" || dnsLabelPattern.MatchString(resourceName) {
		return nil
	}
	return fmt.Errorf("resourceName %q is not a valid Azure resource name", resourceName)
}

// ValidateAWSRegion rejects an AWS region that is not a single DNS label. The
// SageMaker path of generative-aws builds its host as
// runtime.sagemaker.<region>.amazonaws.com without checking the region, so
// "attacker.example/" would move the signed request to attacker.example. The
// AWS SDK clients reject such a region themselves; every module that takes a
// region checks it here as well. A single label stays under amazonaws.com.
func ValidateAWSRegion(region string) error {
	if region == "" || dnsLabelPattern.MatchString(region) {
		return nil
	}
	return fmt.Errorf("region %q is not a valid AWS region name", region)
}

// WarnInvalidAllowedBaseURLs logs AllowedBaseURLsEnv entries that can never
// match.
func WarnInvalidAllowedBaseURLs(logger logrus.FieldLogger) {
	raw := os.Getenv(AllowedBaseURLsEnv)
	_, invalidEntries := ParseAllowedOrigins(raw)
	for _, entry := range invalidEntries {
		logger.WithField("env", AllowedBaseURLsEnv).
			Warnf("ignoring entry %q: expected scheme://host[:port] without wildcards", entry)
	}
	for _, rawEntry := range strings.Split(raw, ",") {
		entry := strings.TrimSpace(rawEntry)
		parsed, err := url.Parse(entry)
		if entry == "" || slices.Contains(invalidEntries, entry) || err != nil {
			continue
		}
		if parsed.User != nil || strings.Trim(parsed.Path, "/") != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			origin, _ := BaseURLOrigin(entry)
			logger.WithField("env", AllowedBaseURLsEnv).
				Warnf("entry %q allows the whole origin %q: userinfo, path, query and fragment are ignored", entry, origin)
		}
	}
}

// ParseAllowedOrigins parses a comma-separated list of origins. Entries that do
// not parse to an origin, or that contain a wildcard, are returned as invalid.
func ParseAllowedOrigins(raw string) (origins, invalidEntries []string) {
	for _, rawEntry := range strings.Split(raw, ",") {
		entry := strings.TrimSpace(rawEntry)
		if entry == "" {
			continue
		}
		origin, ok := BaseURLOrigin(entry)
		if !ok || strings.Contains(origin, "*") {
			invalidEntries = append(invalidEntries, entry)
			continue
		}
		origins = append(origins, origin)
	}
	return origins, invalidEntries
}

// BaseURLOrigin returns scheme://host[:port] of rawURL with the host lowercased.
// Userinfo, path, query and fragment are dropped, so
// https://allowed.example@attacker.example has the origin
// https://attacker.example.
func BaseURLOrigin(rawURL string) (string, bool) {
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

func hasASCIIHost(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	for i := 0; i < len(parsed.Host); i++ {
		if parsed.Host[i] >= 0x80 {
			return false
		}
	}
	return true
}
