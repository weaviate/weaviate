//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package settings

// ParameterDef defines metadata for a module configuration parameter.
// This struct is used across all vectorizer modules to provide a consistent
// way to define and document configuration parameters for SDK developers
// and end users.
type ParameterDef struct {
	// JSONKey is the primary configuration key name used in the API
	JSONKey string

	// AlternateKeys are deprecated or alternate key names that are checked
	// as fallbacks for backwards compatibility. The primary JSONKey is
	// always checked first.
	AlternateKeys []string

	// DefaultValue is the default value for this parameter if not specified.
	// Can be nil if there is no default.
	DefaultValue interface{}

	// Description is a human-readable description of what this parameter does
	Description string

	// Required indicates whether this parameter must be provided by the user
	Required bool

	// AllowedValues specifies valid values for this parameter (enum-like validation).
	// Can be []string, []int64, or other slice types. If nil, any value is allowed.
	AllowedValues interface{}
}
