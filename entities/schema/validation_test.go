//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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

	_, err = ValidatePropertyName("_additional")
	if err == nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("_id")
	if err == nil {
		t.Fail()
	}

	_, err = ValidatePropertyName("id")
	if err == nil {
		t.Fail()
	}
}
