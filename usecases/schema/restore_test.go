//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestoreClass_WithCircularRefs(t *testing.T) {
	// When restoring a class, there could be circular refs between the classes,
	// thus any validation that checks if linked classes exist would fail on the
	// first class to import. Since we have no control over the order of imports
	// when restoring, we need to relax this validation.

	classes := []*models.Class{
		{
			Class: "Class_A",
			Properties: []*models.Property{{
				Name:     "to_Class_B",
				DataType: []string{"Class_B"},
			}, {
				Name:     "to_Class_C",
				DataType: []string{"Class_C"},
			}},
		},

		{
			Class: "Class_B",
			Properties: []*models.Property{{
				Name:     "to_Class_A",
				DataType: []string{"Class_A"},
			}, {
				Name:     "to_Class_C",
				DataType: []string{"Class_C"},
			}},
		},

		{
			Class: "Class_C",
			Properties: []*models.Property{{
				Name:     "to_Class_A",
				DataType: []string{"Class_A"},
			}, {
				Name:     "to_Class_B",
				DataType: []string{"Class_B"},
			}},
		},
	}

	mgr := newSchemaManager()

	for _, classRaw := range classes {
		schemaBytes, err := json.Marshal(classRaw)
		require.Nil(t, err)

		// for this particular test the sharding state does not matter, so we can
		// initiate any new sharding state
		shardingConfig, err := sharding.ParseConfig(nil, 1)
		require.Nil(t, err)

		nodes := fakeNodes{[]string{"node1", "node2"}}
		shardingState, err := sharding.InitState(classRaw.Class, shardingConfig, nodes)
		require.Nil(t, err)

		shardingBytes, err := shardingState.JSON()
		require.Nil(t, err)

		descriptor := backup.ClassDescriptor{Name: classRaw.Class, Schema: schemaBytes, ShardingState: shardingBytes}
		err = mgr.RestoreClass(context.Background(), &descriptor)
		assert.Nil(t, err, "class passes validation")
	}
}

type fakeNodes struct {
	nodes []string
}

func (f fakeNodes) AllNames() []string {
	return f.nodes
}

func (f fakeNodes) LocalName() string {
	return f.nodes[0]
}
