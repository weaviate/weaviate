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
)

// TestFindCancelTargetTask_IsActive pins that cancel targets every in-flight
// status (STARTED/PREPARING/SWAPPING), not just STARTED — otherwise a
// PREPARING/SWAPPING task returns NO_OP on cancel yet still 409s a follow-up
// PUT, leaving the operator with no way forward.
func TestFindCancelTargetTask_IsActive(t *testing.T) {
	mk := func(status distributedtask.TaskStatus) *distributedtask.Task {
		return activeReindexTask("C:enable-filterable:p:aaaa", "C",
			db.ReindexTypeEnableFilterable, "", status, "p")
	}

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
	} {
		t.Run(status.String()+" is a cancel target", func(t *testing.T) {
			target, payload := findCancelTargetTask(
				[]*distributedtask.Task{mk(status)}, "C", "p", "filterable")
			require.NotNil(t, target, "%s task must be a cancel target", status)
			assert.Equal(t, "C:enable-filterable:p:aaaa", target.ID)
			assert.Equal(t, db.ReindexTypeEnableFilterable, payload.MigrationType)
		})
	}

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusFinished,
		distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
	} {
		t.Run(status.String()+" is NOT a cancel target", func(t *testing.T) {
			target, _ := findCancelTargetTask(
				[]*distributedtask.Task{mk(status)}, "C", "p", "filterable")
			require.Nil(t, target, "terminal %s task must not be a cancel target", status)
		})
	}

	t.Run("wrong index type is not a target", func(t *testing.T) {
		target, _ := findCancelTargetTask(
			[]*distributedtask.Task{mk(distributedtask.TaskStatusStarted)}, "C", "p", "searchable")
		require.Nil(t, target, "enable-filterable does not target the searchable index")
	})
}
