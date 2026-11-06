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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// Pins that each refusal of a partly migrated property tells only a caller who
// may read the collection's tenants how many of them migrated.
func TestPartialRunRefusalsCountTenantsOnlyForTenantReaders(t *testing.T) {
	partial := buildTask(t, "T", distributedtask.TaskStatusCancelled, db.ReindexTaskPayload{
		Collection: "C", Properties: []string{"title"}, MigrationType: db.ReindexTypeChangeTokenization,
		TargetTokenization: models.PropertyTokenizationField,
	}, map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
	partial.SwapDecidedGroups = map[string]time.Time{"a": {}}
	tasks := map[string][]*distributedtask.Task{db.ReindexNamespace: {partial}}
	prop := &models.Property{Name: "title", DataType: []string{"text"}, Tokenization: models.PropertyTokenizationWord, IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true)}
	class := &models.Class{Class: "C", Properties: []*models.Property{prop}, InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true}}

	indexes := func(t *testing.T, authz authorization.Authorizer) *indexesHandlers {
		reader := schemaUC.NewMockSchemaReader(t)
		reader.EXPECT().ReadOnlyClass("C").Return(class)
		return &indexesHandlers{appState: &state.State{
			Authorizer: authz, SchemaManager: &schemaUC.Manager{SchemaReader: reader},
			ReindexSubmitLocks: state.NewReindexSubmitLocks(), Logger: logrus.New(),
			ServerConfig: &config.WeaviateConfig{Config: config.Config{RuntimeReindexEnabled: true}},
		}, taskLister: fakeReindexTaskLister{tasks: tasks}}
	}
	for _, tc := range []struct {
		name     string
		wantCode int
		call     func(t *testing.T, authz authorization.Authorizer) middleware.Responder
	}{
		{name: "upsert", wantCode: http.StatusConflict, call: func(t *testing.T, authz authorization.Authorizer) middleware.Responder {
			return indexes(t, authz).upsertIndex(schema.SchemaObjectsIndexUpsertParams{
				HTTPRequest: httptest.NewRequest(http.MethodPut, "/", nil), ClassName: "C", PropertyName: "title",
				IndexName: "searchable", Body: &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationWhitespace},
			}, nil)
		}},
		{name: "rebuild", wantCode: http.StatusConflict, call: func(t *testing.T, authz authorization.Authorizer) middleware.Responder {
			return indexes(t, authz).rebuildIndex(schema.SchemaObjectsIndexRebuildParams{
				HTTPRequest: httptest.NewRequest(http.MethodPost, "/", nil), ClassName: "C", PropertyName: "title", IndexName: "filterable",
			}, nil)
		}},
		{name: "index delete", wantCode: http.StatusUnprocessableEntity, call: func(t *testing.T, authz authorization.Authorizer) middleware.Responder {
			h := gateWithTasks(partial)
			h.authorizer = authz
			return h.deleteClassPropertyIndex(schema.SchemaObjectsPropertiesDeleteParams{
				HTTPRequest: httptest.NewRequest(http.MethodDelete, "/", nil), ClassName: "C", PropertyName: "title", IndexName: "filterable",
			}, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reader := range []struct {
				authz      authorization.Authorizer
				seesCounts bool
			}{{&authorization.DummyAuthorizer{}, true}, {&collectionsOnly{}, false}} {
				rec := httptest.NewRecorder()
				tc.call(t, reader.authz).WriteResponse(rec, runtime.JSONProducer())
				require.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
				require.Equal(t, reader.seesCounts, strings.Contains(rec.Body.String(), "1 of 2 tenants"), rec.Body.String())
			}
		})
	}
}
