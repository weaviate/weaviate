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

package restrictions

import (
	"sort"
	"strings"
)

// DefaultErrorMessageTemplate is used when RESTRICTIONS_ERROR_MESSAGE is
// unset. Three placeholders are recognized: {restriction}, {value}, and
// {allowed} (rendered as a comma-joined list, alphabetically sorted for
// deterministic output). Kept intentionally generic — tier-specific copy
// ("free tier", "please upgrade", etc.) lives only in operator-provided
// overrides, never in OSS source.
const DefaultErrorMessageTemplate = "{value} is not allowed for {restriction}. Allowed values: {allowed}."

// RenderTemplate substitutes {restriction}, {value} and {allowed}
// placeholders in template. An empty template falls back to
// DefaultErrorMessageTemplate so a missing RESTRICTIONS_ERROR_MESSAGE
// doesn't surface as an empty user-facing message.
//
// The allowed list is rendered as a comma-joined, alphabetically sorted
// string so wire output is deterministic across requests regardless of
// operator-configured order.
func RenderTemplate(template string, restriction RestrictionName, value string, allowed []string) string {
	if template == "" {
		template = DefaultErrorMessageTemplate
	}
	sorted := append([]string(nil), allowed...)
	sort.Strings(sorted)
	out := strings.ReplaceAll(template, "{restriction}", string(restriction))
	out = strings.ReplaceAll(out, "{value}", value)
	out = strings.ReplaceAll(out, "{allowed}", strings.Join(sorted, ", "))
	return out
}
