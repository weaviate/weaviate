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
)

func TestValidateOKClassName(t *testing.T) {
	_, err := ValidateClassName("FooBar")
	if err != nil {
		t.Fail()
	}

	_, err = ValidateClassName("FooBar2")
	if err != nil {
		t.Fail()
	}

	_, err = ValidateClassName("Foo_______bar__with_numbers___1234567890_and_2")
	if err != nil {
		t.Fail()
	}

	_, err = ValidateClassName("C_123456___foo_bar_2")
	if err != nil {
		t.Fail()
	}

	_, err = ValidateClassName("NormalClassNameWithNumber1")
	if err != nil {
		t.Fail()
	}

	_, err = ValidateClassName("Normal__Class__Name__With__Number__1")
	if err != nil {
		t.Fail()
	}

	_, err = ValidateClassName("CClassName")
	if err != nil {
		t.Fail()
	}
}

func TestFailValidateBadClassName(t *testing.T) {
	_, err := ValidateClassName("Foo Bar")
	if err == nil {
		t.Fail()
	}

	_, err = ValidateClassName("foo")
	if err == nil {
		t.Fail()
	}

	_, err = ValidateClassName("fooBar")
	if err == nil {
		t.Fail()
	}

	_, err = ValidateClassName("_foo")
	if err == nil {
		t.Fail()
	}
}

func TestValidateOKPropertyName(t *testing.T) {
	// valid proper names
	_, err := ValidatePropertyName("fooBar")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("fooBar2")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("_fooBar2")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("intField")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("hasAction")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("_foo_bar_2")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("______foo_bar_2")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("___123456___foo_bar_2")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("a_very_Long_property_Name__22_with_numbers_9")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("a_very_Long_property_Name__22_with_numbers_9880888800888800008")
	if err != nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("FooBar")
	if err != nil {
		t.Fail()
	}
}

func TestFailValidateBadPropertyName(t *testing.T) {
	_, err := ValidatePropertyName("foo Bar")
	if err == nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("a_very_Long_property_Name__22_with-dash_9")
	if err == nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("1_FooBar")
	if err == nil {
		t.Fail()
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
