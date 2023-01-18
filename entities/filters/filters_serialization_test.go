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

package filters

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSerializeValue(t *testing.T) {
	t.Run("with a float value", func(t *testing.T) {
		before := Value{
			Value: float64(3),
			Type:  schema.DataTypeNumber,
		}

		bytes, err := json.Marshal(before)
		require.Nil(t, err)

		var after Value
		err = json.Unmarshal(bytes, &after)
		require.Nil(t, err)

		assert.Equal(t, before, after)
	})

	t.Run("with an int value", func(t *testing.T) {
		before := Value{
			Value: int(3),
			Type:  schema.DataTypeInt,
		}

		bytes, err := json.Marshal(before)
		require.Nil(t, err)

		var after Value
		err = json.Unmarshal(bytes, &after)
		require.Nil(t, err)

		assert.Equal(t, before, after)
	})
}
