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
	"maps"
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
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// Pins that the resubmit skip and the partial status agree on which tenants
// already serve the target, across every run since the last flip.
func TestMigratedTenantsAcrossRuns(t *testing.T) {
	ownership := map[string][]string{"n1": {"a", "b", "c"}, "n2": {"a", "b", "c"}}
	plan := upsertPlan{
		migrationType: db.ReindexTypeChangeTokenization, targetTok: models.PropertyTokenizationField,
		bucketStrategy: "mapcollection",
	}
	start := time.Now()
	// run builds a task holding tenants held, of which decided swapped.
	run := func(version uint64, status distributedtask.TaskStatus, mt db.ReindexMigrationType, tok string, held string, decided ...string) *distributedtask.Task {
		task := buildTask(t, "T", status, db.ReindexTaskPayload{
			Collection: "C", Properties: []string{"title"}, MigrationType: mt,
			TargetTokenization: tok, BucketStrategy: "mapcollection",
		}, nil)
		task.Version, task.StartedAt = version, start.Add(time.Duration(version)*time.Minute)
		units := map[string]*distributedtask.Unit{}
		for _, g := range held {
			units[string(g)] = &distributedtask.Unit{ID: string(g), GroupID: string(g)}
		}
		task.SetUnits(units)
		for _, g := range decided {
			if task.SwapDecidedGroups == nil {
				task.SwapDecidedGroups = map[string]time.Time{}
			}
			task.SwapDecidedGroups[g] = time.Time{}
		}
		return task
	}
	changeTok, rebuild := db.ReindexTypeChangeTokenization, db.ReindexTypeRebuildSearchable
	field, whitespace := models.PropertyTokenizationField, models.PropertyTokenizationWhitespace
	failed, cancelled, finished := distributedtask.TaskStatusFailed, distributedtask.TaskStatusCancelled, distributedtask.TaskStatusFinished
	only := func(tenants ...string) map[string][]string { return map[string][]string{"n1": tenants, "n2": tenants} }

	for _, tc := range []struct {
		name  string
		tasks []*distributedtask.Task
		want  map[string][]string
		// status of the filterable entry; migrated and total apply to partial.
		status          string
		migrated, total int64
	}{
		{
			name:   "a failed identical run skips the tenants it decided",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, field, "abc", "a")},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name:   "a cancelled identical run skips them too",
			tasks:  []*distributedtask.Task{run(1, cancelled, changeTok, field, "abc", "a")},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name:   "another target reruns everything",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, whitespace, "abc", "a")},
			want:   ownership,
			status: "partial", migrated: 1, total: 3,
		},
		{
			name: "a tenant a replica still owes is rerun, and still reads partial",
			tasks: []*distributedtask.Task{func() *distributedtask.Task {
				task := run(1, failed, changeTok, field, "abc", "a")
				task.SwapsOwed = map[string][]string{"a": {"n2"}}
				return task
			}()},
			want:   ownership,
			status: "partial", migrated: 1, total: 3,
		},
		{
			name:   "a run that decided nothing skips nothing",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, field, "abc")},
			want:   ownership,
			status: "failed",
		},
		{
			name:   "every tenant decided: rerun them all, the flip still has to land",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, field, "abc", "a", "b", "c")},
			want:   ownership,
			status: "partial", migrated: 3, total: 3,
		},
		{
			name: "a resubmit that swapped nothing keeps the first run's tenants",
			tasks: []*distributedtask.Task{
				run(1, cancelled, changeTok, field, "abc", "a", "b"),
				run(2, cancelled, changeTok, field, "c"),
			},
			want:   only("c"),
			status: "partial", migrated: 2, total: 3,
		},
		{
			name: "a resubmit that swapped some adds them to the first run's",
			tasks: []*distributedtask.Task{
				run(1, cancelled, changeTok, field, "abc", "a"),
				run(2, failed, changeTok, field, "bc", "b"),
			},
			want:   only("c"),
			status: "partial", migrated: 2, total: 3,
		},
		{
			name: "a finished rebuild flips nothing and hides nothing",
			tasks: []*distributedtask.Task{
				run(1, cancelled, changeTok, field, "abc", "a"),
				run(2, finished, rebuild, "", "abc"),
			},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name: "a newer swap to another target takes the tenant back",
			tasks: []*distributedtask.Task{
				run(1, failed, changeTok, field, "abc", "a", "b"),
				run(2, failed, changeTok, whitespace, "abc", "b"),
			},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name: "a finished flip of another target rewrote every tenant",
			tasks: []*distributedtask.Task{
				run(1, failed, changeTok, field, "abc", "a"),
				run(2, finished, changeTok, whitespace, "abc"),
			},
			want:   ownership,
			status: "ready",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, skipTenantsAlreadyMigrated(ownership, "C", "title", plan, tc.tasks))

			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "C", "title", "filterable", db.ReindexRuns(tc.tasks), nil)
			require.Equal(t, tc.status, idx.Status)
			require.Equal(t, tc.migrated, idx.MigratedTenants, "migrated tenants")
			require.Equal(t, tc.total, idx.TotalTenants, "total tenants")
		})
	}
}

// Pins that while a partial run stands, a PUT is refused with the resubmit
// unless it is that resubmit, so no second target can hide the partial run.
// "body" and "summary" pin that a run on one index type refuses a PUT on the
// other: searchable and filterable share the property's tokenization.
func TestUpsertRefusedOnPartlyMigratedIndex(t *testing.T) {
	word := models.PropertyTokenizationWord
	title := &models.Property{
		Name: "title", DataType: []string{"text"}, Tokenization: word,
		IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true),
	}
	body := &models.Property{
		Name: "body", DataType: []string{"text"}, Tokenization: word,
		IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false),
	}
	summary := &models.Property{
		Name: "summary", DataType: []string{"text"}, Tokenization: word,
		IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(false),
	}
	props := map[string]*models.Property{"title": title, "body": body, "summary": summary}
	class := &models.Class{Class: "C", Properties: []*models.Property{title, body, summary}, InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true}}
	partialRun := func(id string, status distributedtask.TaskStatus, prop *models.Property, indexType string, req *models.IndexUpsertRequest) *distributedtask.Task {
		plan, err := resolveUpsertPlan(class, "C", prop, indexType, req, nil, true)
		require.NoError(t, err)
		task := buildTask(t, id, status, db.ReindexTaskPayload{
			Collection: "C", Properties: []string{prop.Name}, MigrationType: plan.migrationType,
			TargetTokenization: plan.targetTok, BucketStrategy: plan.bucketStrategy,
		}, map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
		task.SwapDecidedGroups = map[string]time.Time{"a": {}}
		return task
	}
	resubmit := &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationField}
	partials := []*distributedtask.Task{
		partialRun("T", distributedtask.TaskStatusCancelled, title, "searchable", resubmit),
		partialRun("F", distributedtask.TaskStatusCancelled, body, "filterable", &models.IndexUpsertRequest{}),
		partialRun("S", distributedtask.TaskStatusCancelled, summary, "filterable", resubmit),
	}
	running := partialRun("R", distributedtask.TaskStatusStarted, title, "searchable", resubmit)
	running.SwapDecidedGroups = nil
	running.Version = partials[0].Version + 1
	const titleResubmit = `PUT /v1/schema/C/properties/title/index/searchable -d '{"tokenization":"field"}'`

	for _, tc := range []struct {
		name              string
		property          string
		indexType         string
		body              *models.IndexUpsertRequest
		resubmitIsRunning bool
		refusal           string // the resubmit the refusal names; empty: not refused
	}{
		{name: "back to the schema's value", property: "title", indexType: "searchable", body: &models.IndexUpsertRequest{Tokenization: word}, refusal: titleResubmit},
		{name: "back to it on the other index", property: "title", indexType: "filterable", body: &models.IndexUpsertRequest{Tokenization: word}, refusal: titleResubmit},
		{name: "a third target", property: "title", indexType: "searchable", body: &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationWhitespace}, refusal: titleResubmit},
		{name: "the resubmit", property: "title", indexType: "searchable", body: resubmit},
		{name: "a repeat of the resubmit while it runs joins it", property: "title", indexType: "searchable", body: resubmit, resubmitIsRunning: true},
		{name: "a request naming no tokenization", property: "title", indexType: "searchable", body: &models.IndexUpsertRequest{}},
		{name: "a partial enable-filterable, then searchable with another tokenization", property: "body", indexType: "searchable", body: resubmit, refusal: "PUT /v1/schema/C/properties/body/index/filterable"},
		{name: "a partial filterable retokenize, then enabling searchable at the property's tokenization", property: "summary", indexType: "searchable", body: &models.IndexUpsertRequest{Tokenization: word}, refusal: `PUT /v1/schema/C/properties/summary/index/filterable -d '{"tokenization":"field"}'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := partials
			if tc.resubmitIsRunning {
				tasks = append([]*distributedtask.Task{running}, partials...)
			}
			plan, err := resolveUpsertPlan(class, "C", props[tc.property], tc.indexType, tc.body, tasks, true)
			require.NoError(t, err)
			if tc.resubmitIsRunning {
				require.Equal(t, "R", plan.joinTaskID)
			}
			if tc.refusal == "" {
				require.Empty(t, plan.conflict)
				return
			}
			require.Contains(t, plan.conflict, "1 of 2 tenants")
			require.Contains(t, plan.conflict, tc.refusal)
		})
	}
}

// failApplied runs the FSM's fail apply on task over a schema holding class
// and returns the task as the FSM recorded it.
func failApplied(t *testing.T, task *distributedtask.Task, class *models.Class) *distributedtask.Task {
	raw, err := json.Marshal(task)
	require.NoError(t, err)
	var enc map[string]any
	require.NoError(t, json.Unmarshal(raw, &enc))
	enc["units"] = maps.Collect(task.Units())
	snap, err := json.Marshal(map[string]any{"tasks": map[string]any{db.ReindexNamespace: []any{enc}}})
	require.NoError(t, err)
	m := distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:                  logrus.New(),
		SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{db.ReindexNamespace: db.ReindexDetector{}},
		ReadOnlyClass:           func(string) *models.Class { return class },
	})
	require.NoError(t, m.Restore(snap))
	sub, err := json.Marshal(&cmd.MarkTaskFailedRequest{
		Namespace: db.ReindexNamespace, Id: task.ID, Version: task.Version,
		Error: "rpc error after the leader committed", FailedAtUnixMillis: time.Now().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, m.MarkTaskFailed(&cmd.ApplyRequest{SubCommand: sub}))
	list, err := m.ListDistributedTasks(t.Context())
	require.NoError(t, err)
	require.Len(t, list[db.ReindexNamespace], 1)
	return list[db.ReindexNamespace][0]
}

// A run whose cutover write landed while its proposer saw an error, with a
// swap still owed: dropping and re-adding its index must lock nothing, and the
// re-add must rebuild every tenant.
func TestDropAndReAddAfterALandedRun(t *testing.T) {
	cases := []struct {
		name   string
		mt     db.ReindexMigrationType
		tok    string
		idx    string
		landed func() *models.Property
		drop   func(*models.Property)
		readd  *models.IndexUpsertRequest
	}{
		{
			"enable-filterable", db.ReindexTypeEnableFilterable, "", "filterable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexFilterable = boolPtr(false) }, &models.IndexUpsertRequest{},
		},
		{
			"enable-searchable", db.ReindexTypeEnableSearchable, "word", "searchable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true), SearchableBlockmax: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexSearchable, p.SearchableBlockmax = boolPtr(false), nil }, &models.IndexUpsertRequest{Tokenization: "word"},
		},
		{
			"enable-rangeable", db.ReindexTypeEnableRangeable, "", "rangeable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"int"}, IndexFilterable: boolPtr(true), IndexRangeFilters: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexRangeFilters = boolPtr(false) }, &models.IndexUpsertRequest{},
		},
		{
			"change-algorithm", db.ReindexTypeChangeAlgorithm, "", "searchable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true), SearchableBlockmax: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexSearchable, p.SearchableBlockmax = boolPtr(false), nil }, nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.landed()
			class := &models.Class{Class: "C", Properties: []*models.Property{p}, InvertedIndexConfig: &models.InvertedIndexConfig{}}

			run := buildTask(t, "T1", distributedtask.TaskStatusSwapping,
				db.ReindexTaskPayload{Collection: "C", Properties: []string{"p"}, MigrationType: tc.mt, TargetTokenization: tc.tok},
				map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
			run.SwapDecidedGroups = map[string]time.Time{"a": {}, "b": {}}
			run.SwapsOwed = map[string][]string{"b": {"n2"}}
			tasks := []*distributedtask.Task{failApplied(t, run, class)}

			require.NoError(t, db.ReindexDetector{}.CheckPropertyUpdate("C", "p", tasks), "the drop")
			tc.drop(p)
			require.NoError(t, db.ReindexDetector{}.CheckPropertyUpdate("C", "p", tasks), "a change after the drop")
			_, err := (&indexesHandlers{}).resolveRebuildPlan(class, p, "filterable", tasks, true)
			require.NotErrorIs(t, err, errRebuildPropertyPartlyMigrated)
			if tc.readd == nil {
				return
			}
			plan, err := resolveUpsertPlan(class, "C", p, tc.idx, tc.readd, tasks, true)
			require.NoError(t, err)
			require.Empty(t, plan.conflict)
			require.Equal(t, tc.mt, plan.migrationType)
			got := skipTenantsAlreadyMigrated(map[string][]string{"n1": {"a", "b"}, "n2": {"a", "b"}}, "C", "p", plan, tasks)
			require.ElementsMatch(t, []string{"a", "b"}, got["n1"])
		})
	}
}

// Pins that each refusal of a partly migrated property tells only a caller who
// may read the collection's tenants how many of them migrated.
func TestPartialRunRefusalsCountTenantsOnlyForTenantReaders(t *testing.T) {
	partial := partlyMigratedRun(t, "title", db.ReindexTypeChangeTokenization, models.PropertyTokenizationField, "a", "b")
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
