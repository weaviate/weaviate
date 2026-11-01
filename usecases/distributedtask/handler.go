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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

type Handler struct {
	authorizer           authorization.Authorizer
	rbacConfig           rbacconf.Config
	taskLister           distributedtask.TaskLister
	collectionExtractors map[string]distributedtask.CollectionExtractor
}

func NewHandler(authorizer authorization.Authorizer, rbacConfig rbacconf.Config, taskLister distributedtask.TaskLister,
	collectionExtractors map[string]distributedtask.CollectionExtractor,
) *Handler {
	return &Handler{
		authorizer:           authorizer,
		rbacConfig:           rbacConfig,
		taskLister:           taskLister,
		collectionExtractors: collectionExtractors,
	}
}

// ListTasks hides tasks on collections the caller can't read, and redacts
// tenant/shard names unless the caller can read all of the collection's tenants.
func (h *Handler) ListTasks(ctx context.Context, principal *models.Principal) (models.DistributedTasks, error) {
	if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Cluster()); err != nil {
		return nil, err
	}

	tasksByNamespace, err := h.taskLister.ListDistributedTasks(ctx)
	if err != nil {
		return nil, fmt.Errorf("list distributed tasks: %w", err)
	}

	resp := models.DistributedTasks{}
	for namespace, tasks := range tasksByNamespace {
		visible, hideTenants := h.filterTasks(ctx, principal, namespace, tasks)
		resp[namespace] = make([]models.DistributedTask, 0, len(visible))
		for _, task := range visible {
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
				FinishedAt: optionalDateTime(task.FinishedAt),
				Payload:    payload,
			}

			dt.Units = mapUnits(task)
			if hideTenants[task] {
				h.redactTenants(&dt, payload)
			}

			resp[namespace] = append(resp[namespace], dt)
		}
	}

	return resp, nil
}

type collectionTask struct {
	task       *distributedtask.Task
	collection string
}

// filterTasks drops tasks on unreadable collections and flags survivors
// needing tenant-name redaction when the caller lacks tenant read.
func (h *Handler) filterTasks(ctx context.Context, principal *models.Principal, namespace string,
	tasks []*distributedtask.Task,
) (visible []*distributedtask.Task, hideTenants map[*distributedtask.Task]bool) {
	extract := h.collectionExtractors[namespace]
	if extract == nil {
		return tasks, nil
	}

	visible = make([]*distributedtask.Task, 0, len(tasks))
	hideTenants = make(map[*distributedtask.Task]bool, len(tasks))
	bound := make([]collectionTask, 0, len(tasks))
	for _, task := range tasks {
		if collection, ok := extract(task.Payload); ok {
			bound = append(bound, collectionTask{task: task, collection: collection})
		} else {
			// No collection to check tenant read against.
			visible = append(visible, task)
			hideTenants[task] = true
		}
	}

	resourceFilter := filter.New[collectionTask](h.authorizer, h.rbacConfig)
	readable := resourceFilter.Filter(ctx, principal, bound, authorization.READ, func(t collectionTask) string {
		return authorization.CollectionsMetadata(t.collection)[0]
	})
	withTenants := resourceFilter.Filter(ctx, principal, readable, authorization.READ, func(t collectionTask) string {
		return authorization.ShardsMetadata(t.collection)[0]
	})

	for _, t := range readable {
		visible = append(visible, t.task)
		hideTenants[t.task] = true
	}
	for _, t := range withTenants {
		delete(hideTenants, t.task)
	}
	return visible, hideTenants
}

const redactedError = "hidden: requires read permission on the collection's tenants"

// redactTenants strips array/map payload fields, since that's where tenant
// and shard names live, and clears unit IDs and errors, which can embed them.
func (h *Handler) redactTenants(dt *models.DistributedTask, payload map[string]interface{}) {
	for key, value := range payload {
		switch value.(type) {
		case []interface{}, map[string]interface{}:
			delete(payload, key)
		}
	}
	if dt.Error != "" {
		dt.Error = redactedError
	}
	for _, u := range dt.Units {
		u.ID = ""
		if u.Error != "" {
			u.Error = redactedError
		}
	}
}

func mapUnits(task *distributedtask.Task) []*models.DistributedTaskUnit {
	units := make([]*models.DistributedTaskUnit, 0, len(task.Units))
	for _, u := range task.Units {
		units = append(units, &models.DistributedTaskUnit{
			ID:         u.ID,
			NodeID:     u.NodeID,
			Status:     string(u.Status),
			Progress:   u.Progress,
			Error:      u.Error,
			UpdatedAt:  optionalDateTime(u.UpdatedAt),
			FinishedAt: optionalDateTime(u.FinishedAt),
		})
	}
	sort.Slice(units, func(i, j int) bool {
		return units[i].ID < units[j].ID
	})
	return units
}

// optionalDateTime nils out a zero time so the JSON response omits it — a
// struct-typed strfmt.DateTime doesn't get omitempty support for free.
func optionalDateTime(t time.Time) *strfmt.DateTime {
	if t.IsZero() {
		return nil
	}
	dt := strfmt.DateTime(t)
	return &dt
}
