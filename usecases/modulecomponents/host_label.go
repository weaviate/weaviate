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

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

var hostLabelPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

// IsHostLabel reports whether s is one DNS label: 1 to 63 ASCII letters,
// digits and hyphens, with no hyphen at either end. A setting that a module
// pastes into a request host must pass it: a ".", ":", "/", "?", "#" or "@"
// in the setting would send the request, and the key it carries, to another
// host.
func IsHostLabel(s string) bool {
	return hostLabelPattern.MatchString(s)
}

// ValidateAzureResourceName returns an ErrModulePrecondition unless
// resourceName is one DNS label. The Azure OpenAI host is
// <resourceName>.openai.azure.com.
func ValidateAzureResourceName(resourceName string) error {
	if !IsHostLabel(resourceName) {
		return enterrors.NewErrModulePrecondition(fmt.Errorf("resourceName must be 1 to 63 letters, digits or hyphens, with no hyphen at either end, got %q", resourceName))
	}
	return nil
}
