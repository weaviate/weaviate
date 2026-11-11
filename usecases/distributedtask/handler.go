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
	"slices"
	"sort"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

type Handler struct {
	authorizer           authorization.Authorizer
	taskLister           distributedtask.TaskLister
	collectionExtractors map[string]distributedtask.CollectionExtractor
	visiblePayloadFields map[string][]string
}

func NewHandler(authorizer authorization.Authorizer, taskLister distributedtask.TaskLister,
	collectionExtractors map[string]distributedtask.CollectionExtractor, visiblePayloadFields map[string][]string,
) *Handler {
	return &Handler{
		authorizer:           authorizer,
		taskLister:           taskLister,
		collectionExtractors: collectionExtractors,
		visiblePayloadFields: visiblePayloadFields,
	}
}

// ListTasks must not leak collection, tenant, or shard names the caller lacks read access to.
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
		visible, redactTo, err := h.filterTasks(ctx, principal, namespace, tasks)
		if err != nil {
			return nil, err
		}
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
			if fields, redact := redactTo[task]; redact {
				h.redactTenants(&dt, payload, fields)
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

func (h *Handler) filterTasks(ctx context.Context, principal *models.Principal, namespace string,
	tasks []*distributedtask.Task,
) (visible []*distributedtask.Task, redactTo map[*distributedtask.Task][]string, err error) {
	extract := h.collectionExtractors[namespace]
	if extract == nil {
		return tasks, nil, nil
	}

	visible = make([]*distributedtask.Task, 0, len(tasks))
	redactTo = make(map[*distributedtask.Task][]string, len(tasks))
	bound := make([]collectionTask, 0, len(tasks))
	for _, task := range tasks {
		if collection, ok := extract(task.Payload); ok {
			bound = append(bound, collectionTask{task: task, collection: collection})
		} else {
			// Stays listed so a task blocking a 409 is visible, but with no payload
			// fields: a rejected payload can still name a collection.
			visible = append(visible, task)
			redactTo[task] = nil
		}
	}

	readable, err := h.authorizedTasks(ctx, principal, bound, func(c string) string {
		return authorization.CollectionsMetadata(c)[0]
	})
	if err != nil {
		return nil, nil, err
	}
	withTenants, err := h.authorizedTasks(ctx, principal, readable, func(c string) string {
		return authorization.ShardsMetadata(c)[0]
	})
	if err != nil {
		return nil, nil, err
	}

	for _, t := range readable {
		visible = append(visible, t.task)
		redactTo[t.task] = h.visiblePayloadFields[namespace]
	}
	for _, t := range withTenants {
		delete(redactTo, t.task)
	}
	return visible, redactTo, nil
}

// Checks each task's own resource; filter.Filter's shared wildcard would let a tenant-only grant match too.
func (h *Handler) authorizedTasks(ctx context.Context, principal *models.Principal, tasks []collectionTask,
	resource func(collection string) string,
) ([]collectionTask, error) {
	if len(tasks) == 0 {
		return nil, nil
	}
	resources := make([]string, len(tasks))
	for i, t := range tasks {
		resources[i] = resource(t.collection)
	}
	allowed, err := h.authorizer.FilterAuthorizedResources(ctx, principal, authorization.READ, resources...)
	if err != nil {
		return nil, fmt.Errorf("authorize task collections: %w", err)
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, r := range allowed {
		allowedSet[r] = struct{}{}
	}
	kept := make([]collectionTask, 0, len(tasks))
	for i, t := range tasks {
		if _, ok := allowedSet[resources[i]]; ok {
			kept = append(kept, t)
		}
	}
	return kept, nil
}

const redactedError = "hidden: requires read permission on the collection's tenants"

// Payload fields are shown only when allow-listed, so a field added later stays
// hidden by default. Unit IDs and errors can embed tenant and shard names.
func (h *Handler) redactTenants(dt *models.DistributedTask, payload map[string]interface{}, visibleFields []string) {
	for key := range payload {
		if !slices.Contains(visibleFields, key) {
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
