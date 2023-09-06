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

package schema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestShardReplicas(t *testing.T) {
	cache := schemaCache{
		State: State{
			ShardingState: map[string]*sharding.State{},
		},
	}

	// class not found
	_, err := cache.ShardReplicas("C", "S")
	assert.ErrorContains(t, err, "class not found")

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}
	cache.State.ShardingState["C"] = ss
	_, err = cache.ShardReplicas("C", "S")
	assert.ErrorContains(t, err, "shard not found")

	// node with an empty name
	nodes := []string{"A", "B"}
	ss.Physical["S"] = sharding.Physical{BelongsToNodes: nodes}
	res, err := cache.ShardReplicas("C", "S")
	assert.Nil(t, err)
	assert.Equal(t, nodes, res)
}

func TestUpdateClass(t *testing.T) {
	class := "C"
	cache := schemaCache{
		State: State{
			ShardingState: map[string]*sharding.State{},
			ObjectSchema: &models.Schema{
				Classes: []*models.Class{},
			},
		},
	}

	if err := cache.updateClass(&models.Class{}, nil); !errors.Is(err, errClassNotFound) {
		t.Fatalf("update_class: want %v got %v", errClassNotFound, err)
	}
	if _, err := cache.addProperty("?", nil); !errors.Is(err, errClassNotFound) {
		t.Fatalf("add_property: want %v got %v", errClassNotFound, err)
	}
	c := models.Class{
		Class:      class,
		Properties: []*models.Property{},
	}
	uc := models.Class{
		Class: class,
		Properties: []*models.Property{
			{Description: "P1"},
		},
	}
	// add class
	ss := sharding.State{}
	cache.addClass(&c, &ss)

	if c, _ := cache.readOnlyClass(class); c == nil {
		t.Fatalf("class not found")
	} else if c == cache.unsafeFindClass(class) {
		t.Fatalf("read_only_class doesn't return a shallow copy")
	}

	// update class
	cache.updateClass(&uc, &sharding.State{IndexID: class})
	p := models.Property{Description: "P2"}

	x, err := cache.addProperty(class, &p)
	if err != nil {
		t.Fatalf("class not found")
	}
	if n := len(x.Properties); n != 2 {
		t.Fatalf("number of properties want: %v got: 2", n)
	}
}
