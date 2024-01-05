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

package moduletools

type ObjectDiff struct {
	oldVec        []float32
	oldPropValues map[string]interface{}
	newPropValues map[string]interface{}
}

func NewObjectDiff(oldVec []float32) *ObjectDiff {
	return &ObjectDiff{
		oldVec:        oldVec,
		oldPropValues: map[string]interface{}{},
		newPropValues: map[string]interface{}{},
	}
}

func (od *ObjectDiff) WithProp(propName string, oldValue, newValue interface{}) *ObjectDiff {
	od.oldPropValues[propName] = oldValue
	od.newPropValues[propName] = newValue
	return od
}

func (od *ObjectDiff) GetVec() []float32 {
	return od.oldVec
}

func (od *ObjectDiff) IsChangedProp(propName string) bool {
	oldVal, oldExists := od.oldPropValues[propName]
	newVal, newExists := od.newPropValues[propName]

	if !oldExists && !newExists {
		return false
	}
	if !(oldExists && newExists) {
		return true
	}

	// only strings are vectorized, therefore property changes are determined
	// on values of types string, []string and []interface{} which are in fact strings
	switch o := oldVal.(type) {
	case string:
		if n, ok := newVal.(string); ok {
			return n != o
		}
	case []string:
		if ns, ok := newVal.([]string); ok {
			if len(ns) != len(o) {
				return true
			}
			for i := range o {
				if ns[i] != o[i] {
					return true
				}
			}
			return false
		} else if ni, ok := newVal.([]interface{}); ok {
			if len(ni) != len(o) {
				return true
			}
			for i := range o {
				if ni[i] != o[i] {
					return true
				}
			}
			return false
		}
	case []interface{}:
		if ns, ok := newVal.([]string); ok {
			if len(ns) != len(o) {
				return true
			}
			for i := range o {
				if ns[i] != o[i] {
					return true
				}
			}
			return false
		} else if ni, ok := newVal.([]interface{}); ok {
			if len(ni) != len(o) {
				return true
			}
			for i := range o {
				if ni[i] != o[i] {
					return true
				}
			}
			return false
		}
	}

	return true
}
