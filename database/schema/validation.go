/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package schema

import (
	"fmt"
	"regexp"
)

var validateClassNameRegex *regexp.Regexp
var validatePropertyNameRegex *regexp.Regexp

func init() {
	validateClassNameRegex = regexp.MustCompile(`^([A-Z][a-z]+)+$`)
	validatePropertyNameRegex = regexp.MustCompile(`^[a-z]+([A-Z][a-z]+)*$`)
}

// Validates that this string is a valid class name (formate wise)
func ValidateClassName(name string) (error, ClassName) {
	// TODO: Remove hard-coded example
	if name == "WeaviateB/Instrument" {
		return nil, ClassName(name)
	}

	if validateClassNameRegex.MatchString(name) {
		return nil, ClassName(name)
	} else {
		return fmt.Errorf("'%s' is not a valid class name", name), ""
	}
}

// Validates that this string is a valid property name
func ValidatePropertyName(name string) (error, PropertyName) {
	if validatePropertyNameRegex.MatchString(name) {
		return nil, PropertyName(name)
	} else {
		return fmt.Errorf("'%s' is not a valid property name", name), ""
	}
}

// Assert that this string is a valid class name
func AssertValidClassName(name string) ClassName {
	err, n := ValidateClassName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid class name", name))
	}
	return n
}

// Assert that this string is a valid property name
func AssertValidPropertyName(name string) PropertyName {
	err, n := ValidatePropertyName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid property name", name))
	}
	return n
}
