package gremlin

import (
	"fmt"
)

type Vertex struct {
	Id         int
	Label      string
	Properties map[string]Property
}

func (v *Vertex) AssertPropertyValue(name string) *PropertyValue {
	prop := v.PropertyValue(name)

	if prop == nil {
		panic(fmt.Sprintf("Expected to find a property '%v' on vertex '%v', but no such property exists!", name, v.Id))
	}

	return prop
}

func (v *Vertex) PropertyValue(name string) *PropertyValue {
	val, ok := v.Properties[name]
	if !ok {
		return nil
	} else {
		return &val.Value
	}
}
