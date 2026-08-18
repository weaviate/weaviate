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
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// fsmStep is one applied-index position: the task list and the class as this
// node answers them at that point.
type fsmStep struct {
	tasks map[string][]*distributedtask.Task
	class *models.Class
}

// fsmSnapshot models one node's FSM: an applied index that can advance between
// two independent local reads.
//
//	step 0 — task STARTED,  searchable flag off
//	step 1 — task FINISHED, searchable flag on
type fsmSnapshot struct {
	step                int
	advanceBetweenReads bool
	steps               []fsmStep
	// listCalls counts the task reads, so a collection that does not exist
	// can assert it took none.
	listCalls int
}

func (f *fsmSnapshot) LocalDistributedTasks() map[string][]*distributedtask.Task {
	f.listCalls++
	out := f.steps[f.step].tasks
	f.tick()
	return out
}

func (f *fsmSnapshot) ReadOnlyClass(string) *models.Class {
	out := f.steps[f.step].class
	f.tick()
	return out
}

// ClassInfo answers the cheap existence pre-check. It does not advance the
// applied index: the pre-check is not one of the two ordered reads, and
// letting it consume the window would leave that order unpinned.
func (f *fsmSnapshot) ClassInfo(string) clusterSchema.ClassInfo {
	return clusterSchema.ClassInfo{Exists: f.steps[f.step].class != nil}
}

func (f *fsmSnapshot) tick() {
	if f.advanceBetweenReads && f.step == 0 {
		f.step = 1
	}
}

func newFSMSnapshot(t *testing.T) *fsmSnapshot {
	t.Helper()

	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeEnableSearchable,
		Collection:    "C",
		Properties:    []string{"p"},
	}
	tasks := func(status distributedtask.TaskStatus) map[string][]*distributedtask.Task {
		return map[string][]*distributedtask.Task{
			db.ReindexNamespace: {buildTask(t, "C:enable-searchable:p:0001", status, payload, nil)},
		}
	}
	class := func(flagOn bool) *models.Class {
		return &models.Class{
			Class:      "C",
			Properties: []*models.Property{{Name: "p", IndexSearchable: &flagOn}},
		}
	}

	return &fsmSnapshot{steps: []fsmStep{
		{tasks: tasks(distributedtask.TaskStatusStarted), class: class(false)},
		{tasks: tasks(distributedtask.TaskStatusFinished), class: class(true)},
	}}
}

// Tasks must be read before the schema, or an already-read, stale flag-off
// pairs with a FINISHED task read after it and the entry is dropped. A
// collection that does not exist is answered before the task read, which is
// the one thing that read is too expensive to do for nothing: it copies every
// task in every namespace, unit by unit.
func TestReadClassAndTasks_ComeFromOneNodeInOneOrder(t *testing.T) {
	tests := []struct {
		name          string
		advance       bool
		noLister      bool
		noClass       bool
		wantNoTasks   bool
		wantListCalls int
		wantStatus    distributedtask.TaskStatus
		wantFlagOn    bool
	}{
		{
			name:          "schema is never the older operand",
			advance:       true,
			wantListCalls: 1,
			wantStatus:    distributedtask.TaskStatusStarted,
			wantFlagOn:    true,
		},
		{
			name:        "no cluster service",
			noLister:    true,
			wantNoTasks: true,
		},
		{
			name:          "no such collection, so no task read",
			noClass:       true,
			wantNoTasks:   true,
			wantListCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := newFSMSnapshot(t)
			snapshot.advanceBetweenReads = tt.advance
			if tt.noClass {
				for i := range snapshot.steps {
					snapshot.steps[i].class = nil
				}
			}

			var lister localTaskLister
			if !tt.noLister {
				lister = snapshot
			}

			class, parsed := readClassAndTasks("C", lister, snapshot)
			require.Equal(t, tt.wantListCalls, snapshot.listCalls)
			if tt.noClass {
				require.Nil(t, class)
				require.Empty(t, parsed)
				return
			}
			require.NotNil(t, class)
			if tt.wantNoTasks {
				require.Empty(t, parsed)
				return
			}
			require.Len(t, parsed, 1)
			require.Equal(t, tt.wantStatus, parsed[0].task.Status)
			require.Equal(t, tt.wantFlagOn, *class.Properties[0].IndexSearchable)
		})
	}
}

// A nil ClusterService must stay out of the localTaskLister interface: boxed,
// its first call nil-derefs on the promoted method.
func TestGetIndexes_NilClusterService_AnswersSchemaOnly(t *testing.T) {
	reader := schemaUC.NewMockSchemaReader(t)
	reader.EXPECT().ResolveAlias("C").Return("")
	reader.EXPECT().ClassInfo("C").Return(clusterSchema.ClassInfo{Exists: true})
	reader.EXPECT().ReadOnlyClass("C").Return(&models.Class{
		Class:      "C",
		Properties: []*models.Property{{Name: "p", DataType: []string{"text"}}},
	})

	h := &indexesHandlers{appState: &state.State{
		Authorizer:    &authorization.DummyAuthorizer{},
		SchemaManager: &schemaUC.Manager{SchemaReader: reader},
		ServerConfig:  &config.WeaviateConfig{},
		Logger:        logrus.New(),
	}}

	resp := h.getIndexes(schema.SchemaObjectsIndexesGetParams{
		HTTPRequest: httptest.NewRequest(http.MethodGet, "/", nil),
		ClassName:   "C",
	}, &models.Principal{Username: "u"})

	rec := httptest.NewRecorder()
	resp.WriteResponse(rec, runtime.JSONProducer())
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"name":"p"`)
}
