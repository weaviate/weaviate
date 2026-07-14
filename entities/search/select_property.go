//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package search

import (
	"fmt"
	"regexp"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/schema"
)

type SelectProperty struct {
	Name string `json:"name"`

	IsPrimitive bool `json:"isPrimitive"`

	IsObject bool `json:"isObject"`

	// Include the __typename in all the Refs below.
	IncludeTypeName bool `json:"includeTypeName"`

	// Not a primitive nor nested type? Then select these properties.
	Refs []SelectClass `json:"refs"`

	// Nested type? Then select these properties.
	Props []SelectProperty `json:"objs"`
}

type SelectClass struct {
	ClassName            string                `json:"className"`
	RefProperties        SelectProperties      `json:"refProperties"`
	AdditionalProperties additional.Properties `json:"additionalProperties"`
}

// FindSelectClass by specifying the exact class name. The returned pointer
// aliases the slice element and must be treated as read-only.
func (sp SelectProperty) FindSelectClass(className schema.ClassName) *SelectClass {
	for i := range sp.Refs {
		if sp.Refs[i].ClassName == string(className) {
			return &sp.Refs[i]
		}
	}

	return nil
}

// FindSelectProperty by specifying the exact object name. The returned
// pointer aliases the slice element and must be treated as read-only.
func (sp SelectProperty) FindSelectProperty(name string) *SelectProperty {
	for i := range sp.Props {
		if sp.Props[i].Name == name {
			return &sp.Props[i]
		}
	}

	return nil
}

// HasPeer returns true if any of the referenced classes are from the specified
// peer
func (sp SelectProperty) HasPeer(peerName string) bool {
	r := regexp.MustCompile(fmt.Sprintf("^%s__", peerName))
	for _, selectClass := range sp.Refs {
		if r.MatchString(selectClass.ClassName) {
			return true
		}
	}

	return false
}

type SelectProperties []SelectProperty

func (sp SelectProperties) HasRefs() bool {
	for _, p := range sp {
		if len(p.Refs) > 0 {
			return true
		}
	}
	return false
}

func (sp SelectProperties) HasProps() bool {
	for _, p := range sp {
		if len(p.Props) > 0 {
			return true
		}
	}
	return false
}

func (sp SelectProperties) ShouldResolve(path []string) (bool, error) {
	if len(path)%2 != 0 || len(path) == 0 {
		return false, fmt.Errorf("used incorrectly: path must have even number of segments in the form of " +
			"refProp, className, refProp, className, etc")
	}

	// the above gives us the guarantee that path contains at least two elements
	property := path[0]
	class := schema.ClassName(path[1])

	for _, p := range sp {
		if p.IsPrimitive {
			continue
		}

		if p.Name != property {
			continue
		}

		selectClass := p.FindSelectClass(class)
		if selectClass == nil {
			continue
		}

		if len(path) > 2 {
			// we're not done yet, this one's nested
			return selectClass.RefProperties.ShouldResolve(path[2:])
		}

		// we are done and found the path
		return true, nil
	}

	return false, nil
}

// FindProperty returns the first property named propName, or nil. The
// pointer aliases the slice element; treat it as read-only.
//
// Prefer Indexed for repeated lookups on the same instance. The index isn't
// cached here because SelectProperties is copied by value and mutated after
// construction (e.g. extractGroupBy), so a cached map could go stale.
func (sp SelectProperties) FindProperty(propName string) *SelectProperty {
	for i := range sp {
		if sp[i].Name == propName {
			return &sp[i]
		}
	}

	return nil
}

// SelectPropertiesIndex is a name-based lookup index over a SelectProperties
// slice; build via Indexed and reuse for repeated lookups. It holds pointers
// into the source slice, so don't mutate the slice while the index is in
// use; it is otherwise read-only and safe for concurrent readers.
type SelectPropertiesIndex map[string]*SelectProperty

// Indexed builds a SelectPropertiesIndex over sp, keeping the first
// occurrence of duplicate names. A nil or empty sp yields a nil index.
func (sp SelectProperties) Indexed() SelectPropertiesIndex {
	if len(sp) == 0 {
		return nil
	}

	idx := make(SelectPropertiesIndex, len(sp))
	for i := range sp {
		if _, ok := idx[sp[i].Name]; !ok {
			idx[sp[i].Name] = &sp[i]
		}
	}

	return idx
}

// Find returns the property named propName, or nil if none matches.
func (idx SelectPropertiesIndex) Find(propName string) *SelectProperty {
	return idx[propName]
}

func (sp SelectProperties) GetPropertyNames() []string {
	names := make([]string, len(sp))
	for i := range sp {
		names[i] = sp[i].Name
	}
	return names
}
