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

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBSTMulti_Flatten(t *testing.T) {
	t.Run("flattened bst is snapshot of current bst", func(t *testing.T) {
		key1 := "key-1"
		key2 := "key-2"
		key3 := "key-3"
		key4 := "key-4"

		keys := map[string][]byte{
			key1: []byte(key1),
			key2: []byte(key2),
			key3: []byte(key3),
			key4: []byte(key4),
		}
		vals := map[string]value{
			key1: {
				value:     []byte("val-1"),
				tombstone: false,
			},
			key2: {
				value:     []byte("val-2"),
				tombstone: true,
			},
			key3: {
				value:     []byte("val-3"),
				tombstone: false,
			},
		}
		valsUpdated := map[string]value{
			key1: {
				value:     []byte("val-1"),
				tombstone: true,
			},
			key2: {
				value:     []byte("val-22"),
				tombstone: false,
			},
			key3: {
				value:     []byte("val-3"),
				tombstone: true,
			},
			key4: {
				value:     []byte("val-44"),
				tombstone: false,
			},
		}

		type expectedFlattened struct {
			key  []byte
			vals []value
		}
		assertFlattenedMatches := func(t *testing.T, flattened []*binarySearchNodeMulti, expected []expectedFlattened) {
			t.Helper()
			require.Len(t, flattened, len(expected))
			for i, exp := range expected {
				assert.Equal(t, exp.key, flattened[i].key)
				require.Len(t, flattened[i].values, len(exp.vals))
				for j := range exp.vals {
					assert.Equal(t, exp.vals[j].value, flattened[i].values[j].value)
					assert.Equal(t, exp.vals[j].tombstone, flattened[i].values[j].tombstone)
				}
			}
		}

		bst := &binarySearchTreeMulti{}
		// mixed order
		bst.insert(keys[key3], []value{vals[key3]})
		bst.insert(keys[key1], []value{vals[key1]})
		bst.insert(keys[key2], []value{vals[key2]})

		expectedBeforeUpdate := []expectedFlattened{
			{keys[key1], []value{vals[key1]}},
			{keys[key2], []value{vals[key2]}},
			{keys[key3], []value{vals[key3]}},
		}

		flatBeforeUpdate := bst.flattenInOrder()
		assertFlattenedMatches(t, flatBeforeUpdate, expectedBeforeUpdate)

		t.Run("flattened bst does not change on bst update", func(t *testing.T) {
			// mixed order
			bst.insert(keys[key3], []value{valsUpdated[key3]})
			bst.insert(keys[key4], []value{valsUpdated[key4]})
			bst.insert(keys[key1], []value{valsUpdated[key1]})
			bst.insert(keys[key2], []value{valsUpdated[key2]})

			expectedAfterUpdate := []expectedFlattened{
				{keys[key1], []value{vals[key1], valsUpdated[key1]}},
				{keys[key2], []value{vals[key2], valsUpdated[key2]}},
				{keys[key3], []value{vals[key3], valsUpdated[key3]}},
				{keys[key4], []value{valsUpdated[key4]}},
			}

			flatAfterUpdate := bst.flattenInOrder()
			assertFlattenedMatches(t, flatBeforeUpdate, expectedBeforeUpdate)
			assertFlattenedMatches(t, flatAfterUpdate, expectedAfterUpdate)
		})
	})
}
