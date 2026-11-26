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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsHostLabel(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "letters", value: "corp", want: true},
		{name: "one character", value: "a", want: true},
		{name: "digits and hyphens", value: "my-resource-01", want: true},
		{name: "upper case", value: "Corp", want: true},
		{name: "63 characters", value: strings.Repeat("a", 63), want: true},
		{name: "empty", value: ""},
		{name: "64 characters", value: strings.Repeat("a", 64)},
		{name: "fragment", value: "attacker.example#"},
		{name: "path", value: "attacker.example/"},
		{name: "query", value: "attacker.example?"},
		{name: "address with a port and a path", value: "10.0.0.5:8443/x#"},
		{name: "dot", value: "corp.attacker"},
		{name: "userinfo", value: "x@attacker"},
		{name: "underscore", value: "my_resource"},
		{name: "leading hyphen", value: "-corp"},
		{name: "trailing hyphen", value: "corp-"},
		{name: "trailing newline", value: "corp\n"},
		{name: "space", value: "corp x"},
		{name: "percent-encoded slash", value: "corp%2F"},
		{name: "Kelvin sign that lowercases to k", value: "corpK"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsHostLabel(tt.value))
			if tt.want {
				assert.NoError(t, ValidateAzureResourceName(tt.value))
			} else {
				assert.EqualError(t, ValidateAzureResourceName(tt.value),
					"resourceName must be 1 to 63 letters, digits or hyphens, with no hyphen at either end, got "+
						strconv.Quote(tt.value))
			}
		})
	}
}
