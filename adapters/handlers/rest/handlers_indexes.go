package rest

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/nodes"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/userindex"
)

type indexesHandlers struct {
	manager *userindex.Coordinator
}

func (h *indexesHandlers) getAll(
	params schema.SchemaClassesIndexesGetParams,
	principal *models.Principal,
) middleware.Responder {
	res, err := h.manager.Get(params.HTTPRequest.Context(), principal,
		params.ClassName)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return nodes.NewNodesGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case enterrors.ErrUnprocessable:
			return nodes.NewNodesGetUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		case enterrors.ErrNotFound:
			return nodes.NewNodesGetNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return nodes.NewNodesGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return schema.NewSchemaClassesIndexesGetOK().
		WithPayload(res)
}

func setupIndexesHandlers(api *operations.WeaviateAPI,
	repo *db.DB, appState *state.State, httpClient *http.Client,
) {
	indexesManager := userindex.New(repo, appState.Authorizer,
		appState.Cluster, clients.NewUserIndex(httpClient))

	h := &indexesHandlers{indexesManager}
	api.SchemaSchemaClassesIndexesGetHandler = schema.
		SchemaClassesIndexesGetHandlerFunc(h.getAll)
}
