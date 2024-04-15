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
