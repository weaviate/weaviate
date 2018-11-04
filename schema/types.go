package schema

import (
	"fmt"
	"regexp"
)

// Newtype to denote that this string is used as a Class name
type ClassName string

// Newtype to denote that this string is used as a Property name
type PropertyName string

var validateClassNameRegex *regexp.Regexp
var validatePropertyNameRegex *regexp.Regexp

func init() {
	validateClassNameRegex = regexp.MustCompile(`^([A-Z][a-z]+)+$`)
	validatePropertyNameRegex = regexp.MustCompile(`^[a-z]+([A-Z][a-z]+)*$`)
}

func ValidateClassName(name string) (error, ClassName) {
	if validateClassNameRegex.MatchString(name) {
		return nil, ClassName(name)
	} else {
		return fmt.Errorf("'%s' is not a valid class name", name), ""
	}
}

func ValidatePropertyName(name string) (error, PropertyName) {
	if validatePropertyNameRegex.MatchString(name) {
		return nil, PropertyName(name)
	} else {
		return fmt.Errorf("'%s' is not a valid property name", name), ""
	}
}

func AssertValidClassName(name string) ClassName {
	err, n := ValidateClassName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid class name", name))
	}
	return n
}

func AssertValidPropertyName(name string) PropertyName {
	err, n := ValidatePropertyName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid property name", name))
	}
	return n
}
