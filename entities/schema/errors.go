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

package schema

import "errors"

const (
	ErrorNoSuchClass    string = "no such class with name '%s' found in the schema. Check your schema files for which classes are available"
	ErrorNoSuchProperty string = "no such prop with name '%s' found in class '%s' in the schema. Check your schema files for which properties in this class are available"
	ErrorNoSuchDatatype string = "given value-DataType does not exist."
)

var ErrRefToNonexistentClass = errors.New("reference property to nonexistent class")
