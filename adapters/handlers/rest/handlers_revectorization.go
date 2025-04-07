package rest

import (
	"fmt"

	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/revectorization"
	"github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/entities/models"
)

func setupRevectorizationHandlers(api *operations.WeaviateAPI, raft *cluster.Raft) {
	h := &revectorizationHandlers{}

	api.RevectorizationRevectorizationHandler = revectorization.RevectorizationHandlerFunc(h.addNewTask)
}

type revectorizationHandlers struct {
}

func (h *revectorizationHandlers) addNewTask(params revectorization.RevectorizationParams, principal *models.Principal) middleware.Responder {
	fmt.Println(params, principal)
	return revectorization.NewRevectorizationOK().WithPayload(&models.RevectorizationStatusResponse{
		TaskType: "REVECTORIZATION",
		Status:   "STARTED",
		TaskID:   "123",
	})
}
