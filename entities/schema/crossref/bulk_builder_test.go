//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package crossref

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBulkBuilder_EnoughPreAllocation(t *testing.T) {
	className := "MyClass"
	bb := NewBulkBuilderWithEstimates(25, className, 1.00)

	for i := 0; i < 25; i++ {
		id := strfmt.UUID(uuid.New().String())
		res := bb.ClassAndID(className, id)
		expected := []byte(fmt.Sprintf("weaviate://localhost/MyClass/%s", id))
		assert.Equal(t, expected, res)
	}
}

func TestBulkBuilder_NotEnoughPreAllocation(t *testing.T) {
	className := "MyClass"
	bb := NewBulkBuilderWithEstimates(10, className, 1.00)

	for i := 0; i < 25; i++ {
		id := strfmt.UUID(uuid.New().String())
		res := bb.ClassAndID(className, id)
		expected := []byte(fmt.Sprintf("weaviate://localhost/MyClass/%s", id))
		assert.Equal(t, expected, res)
	}
}

func TestBulkBuilder_LegacyWithoutClassName_EnoughPreAllocation(t *testing.T) {
	bb := NewBulkBuilderWithEstimates(25, "", 1.00)

	for i := 0; i < 25; i++ {
		id := strfmt.UUID(uuid.New().String())
		res := bb.LegacyIDOnly(id)
		expected := []byte(fmt.Sprintf("weaviate://localhost/%s", id))
		assert.Equal(t, expected, res)
	}
}

func TestBulkBuilder_LegacyWithoutClassName_NotEnoughPreAllocation(t *testing.T) {
	bb := NewBulkBuilderWithEstimates(10, "", 1.00)

	for i := 0; i < 25; i++ {
		id := strfmt.UUID(uuid.New().String())
		res := bb.LegacyIDOnly(id)
		expected := []byte(fmt.Sprintf("weaviate://localhost/%s", id))
		assert.Equal(t, expected, res)
	}
}
