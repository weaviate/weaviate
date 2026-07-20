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

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// rawTask builds a task with an arbitrary raw payload (undecodable or empty).
func rawTask(id string, status distributedtask.TaskStatus, payload string) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id},
		Status:         status,
		Payload:        []byte(payload),
	}
}

// TestResolveUpsertPlan_NoopFailsClosedOnUndecodableTask pins that a would-be
// NO_OP fails closed (503) when an in-flight task has an undecodable payload,
// rather than returning a false 200.
func TestResolveUpsertPlan_NoopFailsClosedOnUndecodableTask(t *testing.T) {
	on, off := boolPtr(true), boolPtr(false)

	cases := []struct {
		name          string
		indexType     string
		prop          *models.Property
		blockmax      bool
		body          *models.IndexUpsertRequest
		tasks         []*distributedtask.Task
		wantFailClose bool
		wantNoop      bool
	}{
		{
			name:          "searchable algorithm no-op + undecodable in-flight task -> fail closed",
			indexType:     "searchable",
			prop:          textProp("t", "word", on, off),
			blockmax:      true, // class flag on -> algorithm blockmax would NO_OP
			body:          &models.IndexUpsertRequest{Algorithm: "blockmax"},
			tasks:         []*distributedtask.Task{rawTask("C:mystery:t:aaaa", distributedtask.TaskStatusStarted, "{not valid json")},
			wantFailClose: true,
		},
		{
			name:          "searchable algorithm no-op + empty-fields in-flight task -> fail closed",
			indexType:     "searchable",
			prop:          textProp("t", "word", on, off),
			blockmax:      true,
			body:          &models.IndexUpsertRequest{Algorithm: "blockmax"},
			tasks:         []*distributedtask.Task{rawTask("C:mystery:t:bbbb", distributedtask.TaskStatusStarted, "{}")},
			wantFailClose: true,
		},
		{
			name:      "searchable algorithm no-op + no undecodable task -> NO_OP",
			indexType: "searchable",
			prop:      textProp("t", "word", on, off),
			blockmax:  true,
			body:      &models.IndexUpsertRequest{Algorithm: "blockmax"},
			tasks:     nil,
			wantNoop:  true,
		},
		{
			name:          "filterable no-op + undecodable in-flight task -> fail closed",
			indexType:     "filterable",
			prop:          textProp("t", "word", on, on),
			body:          &models.IndexUpsertRequest{},
			tasks:         []*distributedtask.Task{rawTask("C:mystery:t:cccc", distributedtask.TaskStatusStarted, "not json at all")},
			wantFailClose: true,
		},
		{
			name:          "undecodable but TERMINAL task does not fail a no-op",
			indexType:     "searchable",
			prop:          textProp("t", "word", on, off),
			blockmax:      true,
			body:          &models.IndexUpsertRequest{Algorithm: "blockmax"},
			tasks:         []*distributedtask.Task{rawTask("C:mystery:t:dddd", distributedtask.TaskStatusFinished, "{bad")},
			wantNoop:      true,
			wantFailClose: false,
		},
	}
	h := &indexesHandlers{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(tc.blockmax, tc.prop)
			plan, err := h.resolveUpsertPlan(class, "C", tc.prop, tc.indexType, tc.body, tc.tasks)
			require.NoError(t, err)
			assert.Equal(t, tc.wantFailClose, plan.failClosed, "failClosed")
			assert.Equal(t, tc.wantNoop, plan.noop, "noop")
		})
	}
}
