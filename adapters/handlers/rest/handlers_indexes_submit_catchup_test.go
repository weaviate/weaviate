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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
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

type catchUpLeader struct {
	t          *testing.T
	task       *distributedtask.Task
	versions   map[string]uint64
	versionErr error
	listed     bool
}

func (l *catchUpLeader) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	l.listed = true
	return map[string][]*distributedtask.Task{db.ReindexNamespace: {l.task}}, nil
}

func (l *catchUpLeader) QueryClassVersionsWithContext(context.Context, ...string) (map[string]uint64, error) {
	require.True(l.t, l.listed, "the class version is read after the task list")
	return l.versions, l.versionErr
}

// A lagging follower pairs the leader's task list with its own class only once it applied the leader's class version.
func TestSubmitCatchUp(t *testing.T) {
	const reaches = "cannot submit reindex task"
	ranged := func(on bool) *models.Property {
		return &models.Property{Name: "p", DataType: []string{"int"}, IndexRangeFilters: boolPtr(on)}
	}
	searchable := func(tok string, on bool) *models.Property {
		return blockmaxStamped(textProp("p", tok, boolPtr(on), boolPtr(true)), true)
	}
	finished := func(mt db.ReindexMigrationType, tok string) *distributedtask.Task {
		return buildTask(t, "T", distributedtask.TaskStatusFinished, db.ReindexTaskPayload{
			Collection: "C", Properties: []string{"p"}, MigrationType: mt, TargetTokenization: tok,
		}, nil)
	}
	upsert := func(index string, body *models.IndexUpsertRequest) func(*indexesHandlers) middleware.Responder {
		return func(h *indexesHandlers) middleware.Responder {
			return h.upsertIndex(schema.SchemaObjectsIndexUpsertParams{
				HTTPRequest: httptest.NewRequest(http.MethodPut, "/", nil), ClassName: "C", PropertyName: "p",
				IndexName: index, Body: body,
			}, nil)
		}
	}
	rebuild := func(h *indexesHandlers) middleware.Responder {
		return h.rebuildIndex(schema.SchemaObjectsIndexRebuildParams{
			HTTPRequest: httptest.NewRequest(http.MethodPost, "/", nil), ClassName: "C", PropertyName: "p", IndexName: "searchable",
		}, nil)
	}
	enableRangeable := finished(db.ReindexTypeEnableRangeable, "")
	versions := map[string]uint64{"C": 7}

	tests := []struct {
		name            string
		stale, caughtUp *models.Property
		task            *distributedtask.Task
		versions        map[string]uint64
		versionErr      error
		waitErr         error
		call            func(*indexesHandlers) middleware.Responder
		wantCode        int
		wantBody        string
	}{
		{
			name: "rangeable landed on the leader", stale: ranged(false), caughtUp: ranged(true),
			task: enableRangeable, versions: versions, call: upsert("rangeFilters", nil),
			wantCode: http.StatusOK, wantBody: reindexNoOpStatus,
		},
		{
			name: "tokenization changed on the leader", stale: searchable("word", true), caughtUp: searchable("field", true),
			task: finished(db.ReindexTypeChangeTokenization, "field"), versions: versions,
			call: upsert("searchable", &models.IndexUpsertRequest{Tokenization: "word"}), wantCode: http.StatusServiceUnavailable, wantBody: reaches,
		},
		{
			name: "searchable enabled on the leader, rebuild", stale: searchable("word", false), caughtUp: searchable("word", true),
			task: finished(db.ReindexTypeEnableSearchable, "word"), versions: versions, call: rebuild,
			wantCode: http.StatusServiceUnavailable, wantBody: reaches,
		},
		{
			name: "this node cannot catch up", stale: ranged(false), caughtUp: ranged(true), task: enableRangeable,
			versions: versions, waitErr: context.DeadlineExceeded, call: upsert("rangeFilters", nil),
			wantCode: http.StatusServiceUnavailable, wantBody: "not caught up",
		},
		{
			name: "the leader does not answer", stale: ranged(false), caughtUp: ranged(true), task: enableRangeable,
			versionErr: context.DeadlineExceeded, call: upsert("rangeFilters", nil),
			wantCode: http.StatusServiceUnavailable, wantBody: "schema version from the leader",
		},
		{
			name: "index deleted on the leader after the run finished", stale: ranged(true), caughtUp: ranged(false),
			task: enableRangeable, versions: versions, call: upsert("rangeFilters", nil),
			wantCode: http.StatusServiceUnavailable, wantBody: reaches,
		},
		{
			name: "collection gone on the leader", stale: ranged(false), caughtUp: ranged(false), task: enableRangeable,
			call: upsert("rangeFilters", nil), wantCode: http.StatusNotFound, wantBody: "not found",
		},
		{
			name: "a finished run is not taken as done", stale: ranged(false), caughtUp: ranged(false),
			task: enableRangeable, versions: versions, call: upsert("rangeFilters", nil),
			wantCode: http.StatusServiceUnavailable, wantBody: reaches,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caughtUp := false
			reader := schemaUC.NewMockSchemaReader(t)
			reader.EXPECT().WaitForUpdate(mock.Anything, uint64(7)).RunAndReturn(func(context.Context, uint64) error {
				caughtUp = tt.waitErr == nil
				return tt.waitErr
			}).Maybe()
			reader.EXPECT().ReadOnlyClass("C").RunAndReturn(func(string) *models.Class {
				prop := tt.stale
				if caughtUp {
					prop = tt.caughtUp
				}
				return &models.Class{Class: "C", Properties: []*models.Property{prop}, InvertedIndexConfig: &models.InvertedIndexConfig{}}
			}).Maybe()
			h := &indexesHandlers{appState: &state.State{
				Authorizer: &authorization.DummyAuthorizer{}, SchemaManager: &schemaUC.Manager{SchemaReader: reader},
				ReindexSubmitLocks: state.NewReindexSubmitLocks(), Logger: logrus.New(),
				ServerConfig: &config.WeaviateConfig{Config: config.Config{RuntimeReindexEnabled: true}},
			}, taskLister: &catchUpLeader{t: t, task: tt.task, versions: tt.versions, versionErr: tt.versionErr}}

			rec := httptest.NewRecorder()
			tt.call(h).WriteResponse(rec, runtime.JSONProducer())
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}
