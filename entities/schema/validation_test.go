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

package schema

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateOKClassName(t *testing.T) {
	for _, name := range []string{
		"A",
		"FooBar",
		"FooBar2",
		"Foo_______bar__with_numbers___1234567890_and_2",
		"C_123456___foo_bar_2",
		"NormalClassNameWithNumber1",
		"Normal__Class__Name__With__Number__1",
		"CClassName",
		"ThisClassNameHasExactly255Characters_MaximumAllowed____________________qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ValidateClassName(name)
			assert.NoError(t, err)
		})
	}
}

func TestFailValidateBadClassName(t *testing.T) {
	for _, name := range []string{
		"",
		"Foo Bar",
		"foo",
		"fooBar",
		"_foo",
		"ThisClassNameHasMoreThan255Characters_MaximumAllowed____________________qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890",
		"_String", "string",
		"_DateTime", "dateTime", "datetime",
		"_Int", "int",
		"_Float", "float",
		"_Boolean", "boolean",
		"_ID", "id",
		"_FieldSet", "fieldSet", "fieldset",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ValidateClassName(name)
			assert.Error(t, err)
		})
	}
}

func TestValidateOKPropertyName(t *testing.T) {
	for _, name := range []string{
		"fooBar",
		"fooBar2",
		"_fooBar2",
		"intField",
		"hasAction",
		"_foo_bar_2",
		"______foo_bar_2",
		"___123456___foo_bar_2",
		"a_very_Long_property_Name__22_with_numbers_9",
		"a_very_Long_property_Name__22_with_numbers_9880888800888800008",
		"FooBar",
		"ThisPropertyNameHasExactly231Characters_MaximumAllowed______________________________qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ValidatePropertyName(name)
			assert.NoError(t, err)
		})
	}
}

func TestFailValidateBadPropertyName(t *testing.T) {
	for _, name := range []string{
		"foo Bar",
		"a_very_Long_property_Name__22_with-dash_9",
		"1_FooBar",
		"ThisPropertyNameHasMoreThan231Characters_MaximumAllowed______________________________qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ValidatePropertyName(name)
			assert.Error(t, err)
		})
	}
}

func TestValidateReservedPropertyName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Reserved name: _additional",
			args: args{
				name: "_additional",
			},
			wantErr: true,
		},
		{
			name: "Reserved name: id",
			args: args{
				name: "id",
			},
			wantErr: true,
		},
		{
			name: "Reserved name: _id",
			args: args{
				name: "_id",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateReservedPropertyName(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("ValidateReservedPropertyName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateReservedPropertyNameSuffix(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "ok plain", input: "foo", wantErr: false},
		{name: "ok suffix-like but different", input: "searchable", wantErr: false},
		{name: "ok ends with underscore", input: "foo_", wantErr: false},
		{name: "reject _searchable", input: "foo_searchable", wantErr: true},
		{name: "reject _rangeable", input: "foo_rangeable", wantErr: true},
		{name: "reject _temp", input: "foo_temp", wantErr: true},
		{name: "reject __meta_count", input: "foo__meta_count", wantErr: true},
		{name: "reject _propertyLength", input: "foo_propertyLength", wantErr: true},
		{name: "reject _nullState", input: "foo_nullState", wantErr: true},
		{name: "reject when suffix is the whole prefix of name", input: "bar_temp", wantErr: true},
		{name: "reject name equal to suffix only", input: "_temp", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateReservedPropertyNameSuffix(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateTenantName(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{
			name:        "valid tenant name",
			input:       "ValidTenantName123",
			expectedErr: "",
		},
		{
			name:        "valid tenant name with hyphen",
			input:       "Valid-Tenant-Name",
			expectedErr: "",
		},
		{
			name:        "valid tenant name with underscore",
			input:       "Valid_Tenant_Name",
			expectedErr: "",
		},
		{
			name:        "empty tenant name",
			input:       "",
			expectedErr: "empty tenant name",
		},
		{
			name:        "invalid tenant name with space",
			input:       "Invalid Tenant Name",
			expectedErr: " 'Invalid Tenant Name' is not a valid tenant name. should only contain alphanumeric characters (a-z, A-Z, 0-9), underscore (_), and hyphen (-), with a length between 1 and 64 characters",
		},
		{
			name:        "invalid tenant name with special character",
			input:       "InvalidTenantName!",
			expectedErr: " 'InvalidTenantName!' is not a valid tenant name. should only contain alphanumeric characters (a-z, A-Z, 0-9), underscore (_), and hyphen (-), with a length between 1 and 64 characters",
		},
		{
			name:        "tenant name too long",
			input:       "ThisTenantNameIsWayTooLongAndShouldNotBeValidBecauseItExceedsTheMaximumAllowedLength",
			expectedErr: " 'ThisTenantNameIsWayTooLongAndShouldNotBeValidBecauseItExceedsTheMaximumAllowedLength' is not a valid tenant name. should only contain alphanumeric characters (a-z, A-Z, 0-9), underscore (_), and hyphen (-), with a length between 1 and 64 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTenantName(tt.input)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedErr, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateClassNameIncludesRegex(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{
			name:        "valid class name",
			input:       "ValidClassName",
			expectedErr: "",
		},
		{
			name:        "valid class name",
			input:       "*",
			expectedErr: "",
		},
		{
			name:        "valid class name with regex pattern",
			input:       "ValidClassName.*",
			expectedErr: "",
		},
		{
			name:        "invalid class name with special character",
			input:       "InvalidClassName!",
			expectedErr: "not a valid class name",
		},
		{
			name:        "invalid class name with space",
			input:       "Invalid ClassName",
			expectedErr: "not a valid class name",
		},
		{
			name:        "invalid class name with spaces",
			input:       "InvalidClassName WithSpaces",
			expectedErr: "not a valid class name",
		},
		{
			name:        "class name too long",
			input:       "ThisClassNameHasExactly256Characters_MaximumAllowed____________________qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890_qwertyuiopasdfghjklzxcvbnm1234567890A",
			expectedErr: "not a valid class name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateClassNameIncludesRegex(tt.input)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateTenantNameIncludesRegex(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{
			name:        "valid tenant name",
			input:       "ValidTenantName",
			expectedErr: "",
		},
		{
			name:        "valid tenant name",
			input:       "*",
			expectedErr: "",
		},
		{
			name:        "valid tenant name with hyphen",
			input:       "Valid-Tenant-Name",
			expectedErr: "",
		},
		{
			name:        "valid tenant name with underscore",
			input:       "Valid_Tenant_Name",
			expectedErr: "",
		},
		{
			name:        "empty tenant name",
			input:       "",
			expectedErr: "empty tenant name",
		},
		{
			name:        "invalid tenant name with space",
			input:       "Invalid Tenant Name",
			expectedErr: "not a valid tenant name",
		},
		{
			name:        "invalid tenant name with special character",
			input:       "InvalidTenantName!",
			expectedErr: "not a valid tenant name",
		},
		{
			name:        "tenant name too long",
			input:       "ThisTenantNameIsWayTooLongAndShouldNotBeValidBecauseItExceedsTheMaximumAllowedLength",
			expectedErr: "not a valid tenant name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTenantNameIncludesRegex(tt.input)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestValidateClassName_RejectsNamespaceSeparator locks in the invariant that
// plain class names must not contain NamespaceSeparator (":"). This is the
// *contract* the namespaces startup guard depends on to distinguish
// namespace-qualified classes from plain ones. If ClassNameRegexCore is ever
// loosened to accept ":", this test will start passing for qualified names
// and must be updated together with the consumers of NamespaceSeparator.
func TestValidateClassName_RejectsNamespaceSeparator(t *testing.T) {
	cases := []string{
		"Foo" + NamespaceSeparator + "Bar",
		NamespaceSeparator + "Movie",
		"Movie" + NamespaceSeparator,
		"a" + NamespaceSeparator + "b",
		"Namespace" + NamespaceSeparator + "Movie",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ValidateClassName(name)
			assert.Error(t, err, "class name %q containing NamespaceSeparator must be rejected", name)
		})
	}
}

// TestIndexNameRegexCore covers what IndexNameRegexCore accepts and rejects:
// plain class names, "<namespace>:<class>" with a 3-36 char namespace, and
// the boundary cases (hyphen edges, leading/trailing ":", more than one ":",
// out-of-range namespace lengths).
func TestIndexNameRegexCore(t *testing.T) {
	re := regexp.MustCompile(`^` + IndexNameRegexCore + `$`)

	maxNs := strings.Repeat("a", NamespaceMaxLength)
	tooLongNs := strings.Repeat("a", NamespaceMaxLength+1)
	tooShortNs := strings.Repeat("a", NamespaceMinLength-1)

	accept := []string{
		"Movies",
		"customer1:Movies",
		"tenant-a:My_Class",
		"abc:Movies",
		maxNs + ":Movies",
		"a-b-c:Movies",
		"123:Movies",
	}
	for _, name := range accept {
		t.Run("accept/"+name, func(t *testing.T) {
			assert.True(t, re.MatchString(name), "expected %q to match", name)
		})
	}

	reject := []string{
		":Movies",
		"customer:",
		"customer:1Movies",
		"-abc:Movies",
		"abc-:Movies",
		"Customer:Movies",
		tooShortNs + ":Movies",
		tooLongNs + ":Movies",
		"a:b:Movies", // more than one ":" — no character class in the pattern accepts ":"
		"customer/Movies",
		"customer:Movies/extra",
		"customer:movies",
		"",
	}
	for _, name := range reject {
		t.Run("reject/"+name, func(t *testing.T) {
			assert.False(t, re.MatchString(name), "expected %q to be rejected", name)
		})
	}
}

// TestValidateQualifiedClassName covers the post-resolver validator used by
// the filter parser: plain class names continue to validate, qualified
// "<namespace>:<Class>" names are accepted, and malformed qualified names
// (uppercase namespace, lowercase class, empty parts, oversized namespace,
// double-prefix, hyphen edges) are rejected. The function returns the input
// name unchanged on success — qualification is preserved for downstream
// schema lookups.
func TestValidateQualifiedClassName(t *testing.T) {
	minNs := strings.Repeat("a", NamespaceMinLength)
	maxNs := strings.Repeat("a", NamespaceMaxLength)
	tooShortNs := strings.Repeat("a", NamespaceMinLength-1)
	tooLongNs := strings.Repeat("a", NamespaceMaxLength+1)

	accept := []string{
		"Movies",
		"M",
		"customer1:Movies",
		"abc:M",
		"cust--er:Movies", // consecutive hyphens are allowed in the namespace interior
		"tenant-a:My_Class",
		minNs + ":Movies",
		maxNs + ":Movies",
	}
	for _, name := range accept {
		t.Run("accept/"+name, func(t *testing.T) {
			got, err := ValidateQualifiedClassName(name)
			assert.NoError(t, err)
			assert.Equal(t, ClassName(name), got, "qualified name must be returned unchanged")
		})
	}

	reject := []string{
		"",
		":Movies",
		"customer1:",
		"cust:movies",  // lowercase class
		"Cust:Movies",  // uppercase namespace
		"-cust:Movies", // leading hyphen on namespace
		"cust-:Movies", // trailing hyphen on namespace
		tooShortNs + ":Movies",
		tooLongNs + ":Movies",
		"a:b:Movies",          // double-prefix — only one ":" is allowed
		"customer:1Movies",    // class portion must start with [A-Z_]
		"customer:Movies/bad", // class portion has invalid character
		strings.Repeat("a", NamespaceMaxLength+1+ClassNameMaxLength+1), // exceeds combined cap
	}
	for _, name := range reject {
		t.Run("reject/"+name, func(t *testing.T) {
			_, err := ValidateQualifiedClassName(name)
			assert.Error(t, err, "expected %q to be rejected", name)
		})
	}
}
