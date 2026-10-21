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

package conv

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ValidateStorableRow is pinned to casbin's file adapter in the rbac package's
// TestValidateStorableRowMatchesFileAdapter.
func TestValidateStorableValue(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "plain", value: "alice"},
		{name: "OIDC subject", value: "https://issuer.example/users/a b@c.d#e"},
		{name: "regex", value: "Movie.*(s|x)[0-9]{2}$"},
		{name: "unicode", value: "é日本"},
		// U+2028 is a separator, not a control character, and a row holds it intact.
		{name: "line separator", value: "a\u2028b"},
		{name: "longest", value: strings.Repeat("a", maxStorableValueLength)},
		{name: "too long", value: strings.Repeat("a", maxStorableValueLength+1), wantErr: "longer than"},
		{name: "comma", value: "a,b", wantErr: `','`},
		{name: "quote", value: `a"b`, wantErr: `'"'`},
		{name: "LF", value: "a\nb", wantErr: `'\n'`},
		{name: "CR", value: "a\rb", wantErr: `'\r'`},
		{name: "tab", value: "a\tb", wantErr: `'\t'`},
		{name: "NUL", value: "a\x00b", wantErr: `'\x00'`},
		{name: "C1 control", value: "a\u0085b", wantErr: `'\u0085'`},
		{name: "DEL", value: "a\x7fb", wantErr: `'\x7f'`},
		{name: "terminal escape", value: "\x1b[31mred", wantErr: `'\x1b'`},
		{name: "invalid UTF-8", value: "a\xffb", wantErr: "valid UTF-8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStorableValue(tt.value)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestValidateStorableCharacters checks that the character check has no length
// cap, which a permission's resource needs.
func TestValidateStorableCharacters(t *testing.T) {
	long := strings.Repeat("a", maxStorableValueLength+1)
	assert.NoError(t, ValidateStorableCharacters(long))
	assert.ErrorContains(t, ValidateStorableCharacters(long+","), `','`)
}
