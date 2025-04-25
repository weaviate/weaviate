package rest

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/vectorization"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/revectorization"
)

func setupVectorizationHandlers(api *operations.WeaviateAPI, raft *rCluster.Raft) {
	h := &vectorizationHandlers{
		raft: raft,
	}

	api.VectorizationVectorizationStartHandler = vectorization.VectorizationStartHandlerFunc(h.start)
	api.VectorizationVectorizationGetStatusHandler = vectorization.VectorizationGetStatusHandlerFunc(h.getStatus)
	api.VectorizationVectorizationCancelHandler = vectorization.VectorizationCancelHandlerFunc(h.cancel)
}

type vectorizationHandlers struct {
	raft *rCluster.Raft
}

func (h *vectorizationHandlers) start(params vectorization.VectorizationStartParams, principal *models.Principal) middleware.Responder {
	// TODO: check authn/authz
	tasksByNamespace, err := h.raft.ListDistributedTasks(params.HTTPRequest.Context())
	if err != nil {
		return vectorization.NewVectorizationCancelInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	tasks := tasksByNamespace[revectorization.DistributedTasksNamespace]

	var activeTask int
	for _, t := range tasks {
		if t.Status == distributedtask.TaskStatusStarted {
			activeTask++
		}
	}
	if activeTask > 5 { // TODO: make it configurable
		return vectorization.NewVectorizationStartBadRequest()
	}

	tenantFilter := ""
	if params.Body.TenantFilter != nil {
		tenantFilter = *params.Body.TenantFilter
	}

	taskID := fmt.Sprintf("%s/%s", params.CollectionName, params.TargetVector)
	err = h.raft.AddDistributedTask(params.HTTPRequest.Context(), revectorization.DistributedTasksNamespace, taskID, revectorization.TaskPayload{
		CollectionName:       params.CollectionName,
		TargetVector:         params.TargetVector,
		TenantFilter:         tenantFilter,
		MaximumErrorsPerNode: 50,
	})
	if err != nil {
		return vectorization.NewVectorizationCancelInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return vectorization.NewVectorizationStartOK()
}

type dummyVectorizationStatusResponse struct {
	Status           distributedtask.TaskStatus    `json:"status"`
	ObjectsProcessed uint64                        `json:"objectsProcessed"`
	ObjectsFailed    uint64                        `json:"objectsFailed"`
	ObjectsTotal     uint64                        `json:"objectsTotal"`
	Errors           []revectorization.ObjectError `json:"errors"`
	StartedAt        time.Time                     `json:"startedAt"`
	FinishedAt       time.Time                     `json:"finishedAt,omitempty"`
}

func (h *vectorizationHandlers) getStatus(params vectorization.VectorizationGetStatusParams, principal *models.Principal) middleware.Responder {
	taskID := fmt.Sprintf("%s/%s", params.CollectionName, params.TargetVector)
	task, ok, err := h.findTask(params.HTTPRequest.Context(), taskID)
	if err != nil {
		return vectorization.NewVectorizationGetStatusInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if !ok {
		return vectorization.NewVectorizationGetStatusNotFound()
	}

	resp := dummyVectorizationStatusResponse{
		Status:     task.Status,
		StartedAt:  task.StartedAt,
		FinishedAt: task.FinishedAt,
	}

	var localStates []revectorization.LocalTaskState = nil // TODO: send a request to all nodes in the cluster to fetch local states for the given task
	for _, state := range localStates {
		resp.ObjectsProcessed += state.Stats.Processed
		resp.ObjectsFailed += state.Stats.Failed
		resp.ObjectsTotal = 1 // TODO: somehow come up with this number
		resp.Errors = append(resp.Errors, state.ObjectErrors...)
	}

	return vectorization.NewVectorizationGetStatusOK()
}

func (h *vectorizationHandlers) cancel(params vectorization.VectorizationCancelParams, principal *models.Principal) middleware.Responder {
	taskID := fmt.Sprintf("%s/%s", params.CollectionName, params.TargetVector)
	task, ok, err := h.findTask(params.HTTPRequest.Context(), taskID)
	if err != nil {
		return vectorization.NewVectorizationCancelInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if !ok {
		return vectorization.NewVectorizationCancelNotFound()
	}

	if task.Status != distributedtask.TaskStatusStarted {
		return vectorization.NewVectorizationCancelBadRequest()
	}

	if err := h.raft.CancelDistributedTask(params.HTTPRequest.Context(), revectorization.DistributedTasksNamespace, taskID, task.Version); err != nil {
		return vectorization.NewVectorizationCancelInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return vectorization.NewVectorizationCancelOK()
}

func (h *vectorizationHandlers) findTask(ctx context.Context, taskID string) (*distributedtask.Task, bool, error) {
	tasksByNamespace, err := h.raft.ListDistributedTasks(ctx)
	if err != nil {
		return nil, false, err
	}
	tasks := tasksByNamespace[revectorization.DistributedTasksNamespace]

	for _, t := range tasks {
		if t.ID == taskID {
			return t, true, nil
		}
	}

	return nil, false, nil
}
