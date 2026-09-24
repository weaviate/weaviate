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

package docker

import (
	"fmt"
	"os"
	"strings"
)

// LicenseKeyEnv names the variable that holds the Weaviate license key for
// namespaced test clusters. CI sets it from a repository secret.
const LicenseKeyEnv = "WEAVIATE_LICENSE_KEY"

// LicenseKey returns the license key in WEAVIATE_LICENSE_KEY without
// surrounding whitespace, or an error if it holds no key.
func LicenseKey() (string, error) {
	key := strings.TrimSpace(os.Getenv(LicenseKeyEnv))
	if key == "" {
		return "", fmt.Errorf("%s is not set; namespaced test clusters need a Weaviate license key", LicenseKeyEnv)
	}
	return key, nil
}
