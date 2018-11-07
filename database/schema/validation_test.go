package schema

import (
	"testing"
)

func TestValidateOKClassName(t *testing.T) {
	err, _ := ValidateClassName("FooBar")
	if err != nil {
		t.Fail()
	}
}

func TestFailValidateBadClassName(t *testing.T) {
	err, _ := ValidateClassName("Foo Bar")
	if err == nil {
		t.Fail()
	}

	err, _ = ValidateClassName("foo")
	if err == nil {
		t.Fail()
	}

	err, _ = ValidateClassName("fooBar")
	if err == nil {
		t.Fail()
	}
}

func TestValidateOKPropertyName(t *testing.T) {
	err, _ := ValidatePropertyName("fooBar")
	if err != nil {
		t.Fail()
	}
}

func TestFailValidateBadPropertyName(t *testing.T) {
	err, _ := ValidatePropertyName("foo Bar")
	if err == nil {
		t.Fail()
	}

	err, _ = ValidatePropertyName("Foo")
	if err == nil {
		t.Fail()
	}

	err, _ = ValidatePropertyName("FooBar")
	if err == nil {
		t.Fail()
	}
}
