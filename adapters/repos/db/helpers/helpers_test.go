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

package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucketNestedFromPropNameLSM(t *testing.T) {
	t.Run("basic property name", func(t *testing.T) {
		assert.Equal(t, "property.nested_foo", BucketNestedFromPropNameLSM("foo"))
	})

	t.Run("does not collide with regular property bucket", func(t *testing.T) {
		assert.NotEqual(t, BucketFromPropNameLSM("foo"), BucketNestedFromPropNameLSM("foo"))
	})
}

func TestBucketNestedMetaFromPropNameLSM(t *testing.T) {
	t.Run("basic property name", func(t *testing.T) {
		assert.Equal(t, "property.nestedmeta_foo", BucketNestedMetaFromPropNameLSM("foo"))
	})

	t.Run("does not collide with nested value bucket", func(t *testing.T) {
		assert.NotEqual(t, BucketNestedFromPropNameLSM("foo"), BucketNestedMetaFromPropNameLSM("foo"))
	})
}
