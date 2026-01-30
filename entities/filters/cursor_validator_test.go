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

package filters

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestValidateCursor(t *testing.T) {
	className := schema.ClassName("TestClass")

	t.Run("with valid UUID in after parameter", func(t *testing.T) {
		validUUID := uuid.New().String()
		cursor := &Cursor{
			After: validUUID,
			Limit: 10,
		}
		err := ValidateCursor(className, cursor, 0, nil, nil)
		require.NoError(t, err)
	})

	t.Run("with empty after parameter", func(t *testing.T) {
		cursor := &Cursor{
			After: "",
			Limit: 10,
		}
		err := ValidateCursor(className, cursor, 0, nil, nil)
		require.NoError(t, err)
	})

	t.Run("with nil UUID in after parameter", func(t *testing.T) {
		cursor := &Cursor{
			After: uuid.Nil.String(),
			Limit: 10,
		}
		err := ValidateCursor(className, cursor, 0, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "after parameter cannot be the nil UUID")
	})

	t.Run("with invalid UUID in after parameter", func(t *testing.T) {
		cursor := &Cursor{
			After: "not-a-valid-uuid",
			Limit: 10,
		}
		err := ValidateCursor(className, cursor, 0, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid uuid")
	})

	t.Run("with negative limit", func(t *testing.T) {
		cursor := &Cursor{
			After: "",
			Limit: -1,
		}
		err := ValidateCursor(className, cursor, 0, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit parameter must be set")
	})

	t.Run("with empty class name", func(t *testing.T) {
		cursor := &Cursor{
			After: "",
			Limit: 10,
		}
		err := ValidateCursor("", cursor, 0, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "class parameter cannot be empty")
	})

	t.Run("with offset parameter (incompatible with cursor)", func(t *testing.T) {
		cursor := &Cursor{
			After: "",
			Limit: 10,
		}
		err := ValidateCursor(className, cursor, 10, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "offset")
		assert.Contains(t, err.Error(), "cannot be set with after and limit parameters")
	})

	t.Run("with sort parameter (incompatible with cursor)", func(t *testing.T) {
		cursor := &Cursor{
			After: "",
			Limit: 10,
		}
		sort := []Sort{{Path: []string{"property"}, Order: "asc"}}
		err := ValidateCursor(className, cursor, 0, nil, sort)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sort")
		assert.Contains(t, err.Error(), "cannot be set with after and limit parameters")
	})
}
