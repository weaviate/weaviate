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
			var finishedNodes []string
			for node := range task.FinishedNodes {
				finishedNodes = append(finishedNodes, node)
			}
			// sort so it would be more deterministic and easier to test
			sort.Strings(finishedNodes)

			// Try to unmarshal the raw payload into a generic JSON object.
			// If we introduce sensitive information to the payload, we can
			// add another method to Provider to unmarshal the payload and strip all the sensitive data.
			var payload map[string]interface{}
			if err = json.Unmarshal(task.Payload, &payload); err != nil {
				return nil, fmt.Errorf("unmarshal payload: %w", err)
			}

			resp[namespace] = append(resp[namespace], models.DistributedTask{
				ID:            task.ID,
				Version:       int64(task.Version),
				Status:        task.Status.String(),
				Error:         task.Error,
				StartedAt:     strfmt.DateTime(task.StartedAt),
				FinishedAt:    strfmt.DateTime(task.FinishedAt),
				FinishedNodes: finishedNodes,
				Payload:       payload,
			})
		}
	}

	return resp, nil
}
