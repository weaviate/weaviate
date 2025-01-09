//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
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
