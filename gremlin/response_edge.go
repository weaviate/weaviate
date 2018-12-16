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
package gremlin

import (
	"fmt"
)

type Edge struct {
	Id         string
	Label      string
	Properties map[string]PropertyValue
}

func (e *Edge) AssertPropertyValue(name string) *PropertyValue {
	prop := e.PropertyValue(name)

	if prop == nil {
		panic(fmt.Sprintf("Expected to find a property '%v' on edge '%v', but no such property exists!", name, e.Id))
	}

	return prop
}

func (e *Edge) PropertyValue(name string) *PropertyValue {
	val, ok := e.Properties[name]
	if !ok {
		return nil
	} else {
		return &val
	}
}
