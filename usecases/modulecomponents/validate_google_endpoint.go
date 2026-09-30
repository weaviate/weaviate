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
	"regexp"
	"strings"
)

const googleAPIHostSuffix = ".googleapis.com"

var (
	// A DNS name below googleapis.com, e.g. us-central1-aiplatform.googleapis.com
	// or generativelanguage.googleapis.com.
	googleAPIHostPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*\.googleapis\.com$`)
	// A single DNS label, e.g. us-central1 or global.
	googleLocationPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
)

// ValidateGoogleApiEndpoint rejects an apiEndpoint outside Google's API domain.
// The Google modules attach the operator's Google credential to every request
// they send - with USE_GOOGLE_AUTH that is a broadly scoped OAuth token - so an
// endpoint pointing elsewhere hands that credential to a foreign host. Unlike
// ValidateBaseURL this is enforced unconditionally: these modules speak only to
// Google. An empty value means "use the module default".
func ValidateGoogleApiEndpoint(apiEndpoint string) error {
	if apiEndpoint == "" {
		return nil
	}
	if !googleAPIHostPattern.MatchString(strings.ToLower(apiEndpoint)) {
		return fmt.Errorf("apiEndpoint must be a Google API host ending in %s, got %q", googleAPIHostSuffix, apiEndpoint)
	}
	return nil
}

// ValidateGoogleLocation rejects a location or region that is not a plain
// Google region name. The Google modules interpolate it into the request host
// (<region>-aiplatform.googleapis.com), so a value carrying a "/" or an "@" can
// move the request off Google's domain the same way an apiEndpoint can. The
// property name is passed in because the modules call it for both "location"
// and "region". An empty value means "use the module default".
func ValidateGoogleLocation(property, location string) error {
	if location == "" {
		return nil
	}
	if !googleLocationPattern.MatchString(strings.ToLower(location)) {
		return fmt.Errorf("%s must be a Google region name, got %q", property, location)
	}
	return nil
}
