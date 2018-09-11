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
