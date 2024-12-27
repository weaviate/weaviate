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

package cuvs_index

type (
	weaviateId = uint64
	cuvsId     = uint32
)

type BiMap struct {
	forward  map[cuvsId]weaviateId
	backward map[weaviateId]cuvsId
}

// Create new BiMap
func NewBiMap() *BiMap {
	return &BiMap{
		forward:  make(map[cuvsId]weaviateId),
		backward: make(map[weaviateId]cuvsId),
	}
}

// Insert a pair
func (b *BiMap) Insert(CuvsId cuvsId, WeaviateId weaviateId) {
	// Remove old mappings if they exist to maintain bijection
	if oldValue, exists := b.forward[CuvsId]; exists {
		delete(b.backward, oldValue)
	}
	if oldKey, exists := b.backward[WeaviateId]; exists {
		delete(b.forward, oldKey)
	}

	b.forward[CuvsId] = WeaviateId
	b.backward[WeaviateId] = CuvsId
}

// Get value by key
func (b *BiMap) GetWeaviateId(cuvsId cuvsId) (weaviateId, bool) {
	value, exists := b.forward[cuvsId]
	return value, exists
}

// Get key by value
func (b *BiMap) GetCuvsId(WeaviateId weaviateId) (cuvsId, bool) {
	key, exists := b.backward[WeaviateId]
	return key, exists
}

// Delete a pair by key
func (b *BiMap) DeleteByCuvsId(CuvsId cuvsId) {
	if value, exists := b.forward[CuvsId]; exists {
		delete(b.forward, CuvsId)
		delete(b.backward, value)
	}
}

// Delete a pair by value
func (b *BiMap) DeleteByValue(WeaviateId weaviateId) {
	if key, exists := b.backward[WeaviateId]; exists {
		delete(b.forward, key)
		delete(b.backward, WeaviateId)
	}
}
