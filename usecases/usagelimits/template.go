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

package usagelimits

import (
	"strconv"
	"strings"
)

// DefaultErrorMessageTemplate is used when USAGE_LIMITS_ERROR_MESSAGE is unset.
// Two placeholders are recognized: {limit} (resource type) and {value} (cap).
// All other tokens are passed through unchanged so operators can include
// upgrade URLs or arbitrary text without escaping concerns.
const DefaultErrorMessageTemplate = "{limit} count limit of {value} reached for this instance."

// RenderTemplate substitutes {limit} and {value} placeholders in template.
// An empty template falls back to DefaultErrorMessageTemplate so a missing
// USAGE_LIMITS_ERROR_MESSAGE doesn't surface as an empty user-facing message.
func RenderTemplate(template string, limit LimitName, value int64) string {
	if template == "" {
		template = DefaultErrorMessageTemplate
	}
	out := strings.ReplaceAll(template, "{limit}", string(limit))
	out = strings.ReplaceAll(out, "{value}", strconv.FormatInt(value, 10))
	return out
}
