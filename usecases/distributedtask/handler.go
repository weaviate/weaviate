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

package distributedtask

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

type Handler struct {
	authorizer  authorization.Authorizer
	tasksLister distributedtask.TasksLister
}

func NewHandler(authorizer authorization.Authorizer, taskLister distributedtask.TasksLister) *Handler {
	return &Handler{
		authorizer:  authorizer,
		tasksLister: taskLister,
	}
}

// ListTasks converts internal [distributedtask.Task] structs into the REST API model.
// The raw task payload (opaque bytes internally) is unmarshaled into a generic JSON map —
// if we ever add sensitive fields to payloads, the [distributedtask.Provider] interface
// should grow a method to redact them before they reach this layer.
func (h *Handler) ListTasks(ctx context.Context, principal *models.Principal) (models.DistributedTasks, error) {
	if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Cluster()); err != nil {
		return nil, err
	}

	tasksByNamespace, err := h.tasksLister.ListDistributedTasks(ctx)
	if err != nil {
		return nil, fmt.Errorf("list distributed tasks: %w", err)
	}

	resp := models.DistributedTasks{}
	for namespace, tasks := range tasksByNamespace {
		resp[namespace] = make([]models.DistributedTask, 0, len(tasks))
		for _, task := range tasks {
			// Try to unmarshal the raw payload into a generic JSON object.
			// If we introduce sensitive information to the payload, we can
			// add another method to Provider to unmarshal the payload and strip all the sensitive data.
			var payload map[string]interface{}
			if err = json.Unmarshal(task.Payload, &payload); err != nil {
				return nil, fmt.Errorf("unmarshal payload: %w", err)
			}

			dt := models.DistributedTask{
				ID:         task.ID,
				Version:    int64(task.Version),
				Status:     task.Status.String(),
				Error:      task.Error,
				StartedAt:  strfmt.DateTime(task.StartedAt),
				FinishedAt: strfmt.DateTime(task.FinishedAt),
				Payload:    payload,
			}

			dt.SubUnits = mapSubUnits(task)

			resp[namespace] = append(resp[namespace], dt)
		}
	}

	return resp, nil
}

func mapSubUnits(task *distributedtask.Task) []*models.DistributedTaskSubUnit {
	subUnits := make([]*models.DistributedTaskSubUnit, 0, len(task.SubUnits))
	for _, su := range task.SubUnits {
		subUnits = append(subUnits, &models.DistributedTaskSubUnit{
			ID:         su.ID,
			NodeID:     su.NodeID,
			Status:     string(su.Status),
			Progress:   su.Progress,
			Error:      su.Error,
			UpdatedAt:  strfmt.DateTime(su.UpdatedAt),
			FinishedAt: strfmt.DateTime(su.FinishedAt),
		})
	}
	sort.Slice(subUnits, func(i, j int) bool {
		return subUnits[i].ID < subUnits[j].ID
	})
	return subUnits
}
