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
	"encoding/json"
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

// advancingFSM models one node's FSM: an applied index that can advance between
// two independent local reads.
//
//	step 0 — task STARTED,  searchable flag off
//	step 1 — task FINISHED, searchable flag on
type advancingFSM struct {
	step                int
	advanceBetweenReads bool
	steps               []fsmStep
	// listCalls counts the task reads, so a collection that does not exist
	// can assert it took none.
	listCalls int
}

func (f *advancingFSM) LocalDistributedTasks() map[string][]*distributedtask.Task {
	f.listCalls++
	out := f.steps[f.step].tasks
	f.advance()
	return out
}

func (f *advancingFSM) ReadOnlyClass(string) *models.Class {
	out := f.steps[f.step].class
	f.advance()
	return out
}

// ClassInfo answers the cheap existence pre-check. It does not advance the
// applied index: the pre-check is not one of the two ordered reads, and
// letting it advance the step would leave that order unpinned.
func (f *advancingFSM) ClassInfo(string) clusterSchema.ClassInfo {
	return clusterSchema.ClassInfo{Exists: f.steps[f.step].class != nil}
}

func (f *advancingFSM) advance() {
	if f.advanceBetweenReads && f.step == 0 {
		f.step = 1
	}
}

func newAdvancingFSM(t *testing.T) *advancingFSM {
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

	return &advancingFSM{steps: []fsmStep{
		{tasks: tasks(distributedtask.TaskStatusStarted), class: class(false)},
		{tasks: tasks(distributedtask.TaskStatusFinished), class: class(true)},
	}}
}

// Tasks must be read before the schema, or an already-read, stale flag-off
// pairs with a FINISHED task read after it and the entry is dropped. A
// collection that does not exist is answered before the task read, which is
// the one thing that read is too expensive to do for nothing: it copies every
// task in every namespace, unit by unit.
//
// The pre-check is not the existence check the response is built on. A
// DELETE /v1/schema/{class} applying between the two reads leaves ClassInfo
// answering Exists against a ReadOnlyClass that is already nil, and the
// second check is what turns that into a 404 rather than a collection with
// no properties.
func TestReadClassAndTasks_ComeFromOneNodeInOneOrder(t *testing.T) {
	tests := []struct {
		name           string
		advance        bool
		noLister       bool
		noClass        bool
		deletedBetween bool
		wantNoTasks    bool
		wantListCalls  int
		wantStatus     distributedtask.TaskStatus
		wantFlagOn     bool
	}{
		{
			name:          "the class is never the older of the two reads",
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
			wantListCalls: 0,
		},
		{
			// The pre-check saw the collection, the task read let the DELETE
			// through, and the class read came back empty-handed.
			name:           "the collection is deleted between the two reads",
			advance:        true,
			deletedBetween: true,
			wantListCalls:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsm := newAdvancingFSM(t)
			fsm.advanceBetweenReads = tt.advance
			if tt.noClass {
				for i := range fsm.steps {
					fsm.steps[i].class = nil
				}
			}
			if tt.deletedBetween {
				fsm.steps[1].class = nil
			}

			var lister localTaskLister
			if !tt.noLister {
				lister = fsm
			}

			class, parsed := readClassAndTasks("C", lister, fsm)
			require.Equal(t, tt.wantListCalls, fsm.listCalls)
			if tt.noClass || tt.deletedBetween {
				require.Nil(t, class,
					"a class the second read cannot find must not be answered with an empty one")
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
	require.Nil(t, resolveTaskSource(&state.State{}),
		"a nil ClusterService must produce a nil interface, not a boxed nil")

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

// staticTasks answers every task read with the same map.
type staticTasks map[string][]*distributedtask.Task

func (s staticTasks) LocalDistributedTasks() map[string][]*distributedtask.Task {
	return s
}

// The task read is cluster-wide: it hands the handler every reindex task this
// node has applied, whatever collection each one names. Which of them the
// response may speak for is decided further down, and both places that decide
// it are here — the entry a running task drives, and the algorithm a completed
// one resolves.
func TestGetIndexes_AForeignCollectionsTaskReachesNoEntry(t *testing.T) {
	const taskID = "X:change-algorithm:p:0001"

	for _, tt := range []struct {
		name           string
		taskCollection string
		taskStatus     distributedtask.TaskStatus
		wantStatus     string
		wantTaskID     string
		wantAlgorithm  string
	}{
		{
			name: "a running migration on this collection drives the entry", taskCollection: "C",
			taskStatus: distributedtask.TaskStatusStarted,
			wantStatus: "indexing", wantTaskID: taskID, wantAlgorithm: models.IndexStatusAlgorithmWand,
		},
		{
			name: "a running migration on another collection does not", taskCollection: "D",
			taskStatus: distributedtask.TaskStatusStarted,
			wantStatus: "ready", wantTaskID: "", wantAlgorithm: models.IndexStatusAlgorithmWand,
		},
		{
			name: "a completed migration on this collection resolves the algorithm", taskCollection: "C",
			taskStatus: distributedtask.TaskStatusFinished,
			wantStatus: "ready", wantTaskID: "", wantAlgorithm: models.IndexStatusAlgorithmBlockmax,
		},
		{
			name: "a completed migration on another collection does not", taskCollection: "D",
			taskStatus: distributedtask.TaskStatusFinished,
			wantStatus: "ready", wantTaskID: "", wantAlgorithm: models.IndexStatusAlgorithmWand,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flagOn := true
			reader := schemaUC.NewMockSchemaReader(t)
			reader.EXPECT().ResolveAlias("C").Return("")
			reader.EXPECT().ClassInfo("C").Return(clusterSchema.ClassInfo{Exists: true})
			reader.EXPECT().ReadOnlyClass("C").Return(&models.Class{
				Class: "C",
				Properties: []*models.Property{
					{Name: "p", DataType: []string{"text"}, IndexSearchable: &flagOn},
				},
			})

			task := buildTask(t, taskID, tt.taskStatus,
				db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeAlgorithm,
					Collection:    tt.taskCollection,
					Properties:    []string{"p"},
				},
				map[string]*distributedtask.Unit{
					"u": {ID: "u", Status: distributedtask.UnitStatusInProgress, Progress: 0.5},
				},
			)

			h := &indexesHandlers{
				appState: &state.State{
					Authorizer:    &authorization.DummyAuthorizer{},
					SchemaManager: &schemaUC.Manager{SchemaReader: reader},
					ServerConfig:  &config.WeaviateConfig{},
					Logger:        logrus.New(),
				},
				taskSource: staticTasks{db.ReindexNamespace: {task}},
			}

			resp := h.getIndexes(schema.SchemaObjectsIndexesGetParams{
				HTTPRequest: httptest.NewRequest(http.MethodGet, "/", nil),
				ClassName:   "C",
			}, &models.Principal{Username: "u"})

			rec := httptest.NewRecorder()
			resp.WriteResponse(rec, runtime.JSONProducer())
			require.Equal(t, http.StatusOK, rec.Code)

			var body models.IndexStatusResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Properties, 1)

			var searchable *models.IndexStatus
			for _, idx := range body.Properties[0].Indexes {
				if idx.Type == "searchable" {
					searchable = idx
				}
			}
			require.NotNil(t, searchable)
			require.Equal(t, tt.wantStatus, searchable.Status)
			require.Equal(t, tt.wantTaskID, searchable.TaskID,
				"only a task naming this collection may put its id on the entry")
			require.Equal(t, tt.wantAlgorithm, searchable.Algorithm,
				"only a completed migration on this collection may resolve the algorithm")
		})
	}
}
