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

package rest

import (
	"errors"

	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/distributed_tasks"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/nodes"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"

	distributedtaskUC "github.com/weaviate/weaviate/usecases/distributedtask"
)

func setupDistributedTasksHandlers(api *operations.WeaviateAPI, authorizer authorization.Authorizer, tasksLister distributedtask.TasksLister) {
	h := distributedTasksHandlers{
		handler: distributedtaskUC.NewHandler(authorizer, tasksLister),
	}

	api.DistributedTasksDistributedTasksGetHandler = distributed_tasks.DistributedTasksGetHandlerFunc(h.getTasks)
}

type distributedTasksHandlers struct {
	handler *distributedtaskUC.Handler
}

func (h *distributedTasksHandlers) getTasks(params distributed_tasks.DistributedTasksGetParams, principal *models.Principal) middleware.Responder {
	tasks, err := h.handler.ListTasks(params.HTTPRequest.Context(), principal)
	if err != nil {
		if errors.As(err, &autherrs.Forbidden{}) {
			return distributed_tasks.NewDistributedTasksGetForbidden()
		}
		return nodes.NewNodesGetClassInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	return distributed_tasks.NewDistributedTasksGetOK().WithPayload(tasks)
}
