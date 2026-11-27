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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func viewTaskState(id string, gen uint64, mt ReindexMigrationType, status distributedtask.TaskStatus,
	switched, hasUnit bool,
) distributedtask.ShardTaskState {
	return distributedtask.ShardTaskState{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Status:         status,
		Header: ReindexTaskHeader{ReindexTaskPayload: ReindexTaskPayload{
			MigrationType: mt, Collection: "ViewTest", Properties: []string{"title"},
			TargetTokenization: models.PropertyTokenizationField, Generation: gen,
		}},
		Switched: switched, HasUnit: hasUnit,
	}
}

func TestReindexClusterView(t *testing.T) {
	off := false
	textProp := func(filterable *bool) *models.Property {
		return &models.Property{
			Name: "title", DataType: schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord, IndexFilterable: filterable,
		}
	}
	const (
		running   = distributedtask.TaskStatusStarted
		cancelled = distributedtask.TaskStatusCancelled
		retok     = ReindexTypeChangeTokenization
	)
	type served struct {
		gen uint64
		on  bool
	}
	tests := []struct {
		name         string
		filterable   *bool
		live         map[string]uint64
		tasks        []distributedtask.ShardTaskState
		governed     bool
		filter       served
		tokenization string
		staged       []uint64
	}{
		{
			name:     "a switched task serves its generation with its target settings",
			tasks:    []distributedtask.ShardTaskState{viewTaskState("a", 2, retok, running, true, true)},
			governed: true, filter: served{2, true}, tokenization: models.PropertyTokenizationField,
		},
		{
			name:     "an unswitched running unit is staged while generation 0 serves",
			tasks:    []distributedtask.ShardTaskState{viewTaskState("a", 2, retok, running, false, true)},
			governed: true, filter: served{0, true}, tokenization: models.PropertyTokenizationWord, staged: []uint64{2},
		},
		{
			name: "of two switched tasks the higher generation serves",
			tasks: []distributedtask.ShardTaskState{
				viewTaskState("a", 2, ReindexTypeRepairFilterable, cancelled, true, false),
				viewTaskState("b", 3, ReindexTypeRepairFilterable, cancelled, true, false),
			},
			governed: true, filter: served{3, true}, tokenization: models.PropertyTokenizationWord,
		},
		{
			name:       "an index off in the class with no task serves nothing and is not governed",
			filterable: &off,
		},
		{
			name:  "a task without a generation is ignored",
			tasks: []distributedtask.ShardTaskState{viewTaskState("a", 0, retok, running, true, true)},
		},
		{
			name:  "a cancelled task with an unswitched unit stages nothing",
			tasks: []distributedtask.ShardTaskState{viewTaskState("a", 2, retok, cancelled, false, true)},
		},
		{
			name:     "the class settings win when Live equals the switched task's generation",
			live:     map[string]uint64{indexTypeFilterable: 2, indexTypeSearchable: 2},
			tasks:    []distributedtask.ShardTaskState{viewTaskState("a", 2, retok, cancelled, true, false)},
			governed: true, filter: served{2, true}, tokenization: models.PropertyTokenizationWord,
		},
		{
			name:       "an index off in the class with Live at the switched generation serves nothing",
			filterable: &off,
			live:       map[string]uint64{indexTypeFilterable: 2},
			tasks: []distributedtask.ShardTaskState{
				viewTaskState("a", 2, ReindexTypeEnableFilterable, cancelled, true, false),
			},
			governed: true, filter: served{2, false}, tokenization: models.PropertyTokenizationWord,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			class := &models.Class{Class: "ViewTest", Properties: []*models.Property{textProp(tc.filterable)}}
			view, err := computeReindexView(class, map[string]map[string]uint64{"title": tc.live}, tc.tasks)
			require.NoError(t, err)
			pv, governed := view["title"]
			require.Equal(t, tc.governed, governed)
			if !governed {
				return
			}
			require.Equal(t, tc.filter, served{pv.gen[indexTypeFilterable], pv.on[indexTypeFilterable]})
			require.Equal(t, tc.tokenization, pv.tokenization)
			var staged []uint64
			for _, s := range pv.staged {
				staged = append(staged, s.gen)
			}
			require.Equal(t, tc.staged, staged)
		})
	}
}
