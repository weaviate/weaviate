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

// DefaultErrorMessageTemplate is rendered when RESTRICTIONS_ERROR_MESSAGE
// is unset. Tier-specific copy lives only in operator overrides, not OSS.
const DefaultErrorMessageTemplate = "{value} is not allowed for {restriction}. Allowed values: {allowed}."

// RenderTemplate substitutes {restriction}, {value}, {allowed} in the
// template (empty = default). Allowed is sorted to keep the wire output
// deterministic regardless of operator-configured order.
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
