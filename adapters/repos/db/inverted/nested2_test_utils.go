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

package inverted

import nested2 "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested2"

// NewNestedProperty2ForTest constructs a NestedProperty2 with a caller-supplied
// *AssignResult and Values slice for use in tests outside the inverted package.
// The unexported result field is set here via the package-internal struct
// literal; configs is left nil so all Exists/Anchors entries pass the gate.
// This constructor exists solely to let db-package oracle tests supply a real
// AssignResult without re-exposing the result field on the public struct.
func NewNestedProperty2ForTest(name string, result *nested2.AssignResult, values []NestedValue2) *NestedProperty2 {
	var numFilterable int
	for _, v := range values {
		if v.HasFilterableIndex {
			numFilterable++
		}
	}
	return &NestedProperty2{
		Name:          name,
		result:        result,
		values:        values,
		numFilterable: numFilterable,
	}
}
