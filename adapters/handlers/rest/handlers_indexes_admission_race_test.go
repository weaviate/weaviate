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
	"encoding/json"
	"io"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

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
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// raceTaskService stands in for the RAFT task service. onCommitted fires where
// the real leader would make the task visible to every backup's check, which
// is what lets tests place the race at that exact point.
type raceTaskService struct {
	mu          sync.Mutex
	tasks       []*distributedtask.Task
	cancelled   []distributedtask.TaskDescriptor
	adds        int
	lists       int
	onCommitted func()
	// cancelErr, when set, fails every cancel — the shape of a rollback that
	// never lands.
	cancelErr error
}

func (s *raceTaskService) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	out := make([]*distributedtask.Task, len(s.tasks))
	copy(out, s.tasks)
	return map[string][]*distributedtask.Task{db.ReindexNamespace: out}, nil
}

func (s *raceTaskService) add(taskID string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.adds++
	s.tasks = append(s.tasks, &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: taskID, Version: 3},
		Namespace:      db.ReindexNamespace,
		Status:         distributedtask.TaskStatusStarted,
		Payload:        raw,
	})
	s.mu.Unlock()
	if s.onCommitted != nil {
		s.onCommitted()
	}
	return nil
}

func (s *raceTaskService) AddDistributedTaskWithBarrier(_ context.Context, _, taskID string,
	payload any, _ []string, _ bool,
) error {
	return s.add(taskID, payload)
}

func (s *raceTaskService) AddDistributedTaskWithGroupsBarrier(_ context.Context, _, taskID string,
	payload any, _ []distributedtask.UnitSpec, _ bool,
) error {
	return s.add(taskID, payload)
}

func (s *raceTaskService) CancelDistributedTask(_ context.Context, _, taskID string, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelErr != nil {
		return s.cancelErr
	}
	s.cancelled = append(s.cancelled, distributedtask.TaskDescriptor{ID: taskID, Version: version})
	for _, t := range s.tasks {
		if t.ID == taskID {
			t.Status = distributedtask.TaskStatusCancelled
		}
	}
	return nil
}

func (s *raceTaskService) startedTasks() []*distributedtask.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*distributedtask.Task
	for _, t := range s.tasks {
		if t.Status == distributedtask.TaskStatusStarted {
			out = append(out, t)
		}
	}
	return out
}

// togglingProber answers the backup probe from a flag the test flips, so a
// backup claim can be placed at an exact point in the submission sequence.
// An unset kind reports a backup.
type togglingProber struct {
	busy *atomic.Bool
	kind string
}

func (p togglingProber) NodeActivity(context.Context, string) (backup.NodeActivity, error) {
	if p.busy.Load() {
		kind := p.kind
		if kind == "" {
			kind = backup.NodeActivityKindBackup
		}
		return backup.NodeActivity{Busy: true, Kind: kind, ID: kind + "-1"}, nil
	}
	return backup.NodeActivity{}, nil
}

// fixtureNode is the single node submissionHandlers puts in the cluster.
const fixtureNode = "node1"

// submissionHandlers builds the handler the submission path runs against: one
// collection with one filterable text property, owned by one node.
func submissionHandlers(t *testing.T, tasks reindexTaskService, prober nodeActivityProber) *indexesHandlers {
	t.Helper()
	const (
		collection = "Movies"
		property   = "title"
		node       = fixtureNode
	)

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	indexFilterable := true
	class := &models.Class{
		Class: collection,
		Properties: []*models.Property{{
			Name:            property,
			DataType:        []string{"text"},
			IndexFilterable: &indexFilterable,
		}},
	}
	shardState := &sharding.State{
		IndexID:  collection,
		Physical: map[string]sharding.Physical{"shard1": {Name: "shard1", BelongsToNodes: []string{node}}},
	}

	reader := schemaUC.NewMockSchemaReader(t)
	reader.On("ReadOnlyClass", collection).Return(class).Maybe()
	reader.On("Read", collection, true, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(*models.Class, *sharding.State) error)
		require.NoError(t, fn(class, shardState))
	}).Maybe()

	theDB := &db.DB{}
	theDB.SetSchemaReader(reader)

	return &indexesHandlers{
		appState: &state.State{
			Authorizer:         &authorization.DummyAuthorizer{},
			ReindexSubmitLocks: state.NewReindexSubmitLocks(),
			Logger:             logger,
			// These tests exercise the gate machinery, which only runs when the
			// feature is on; with RUNTIME_REINDEX_ENABLED off every submit is a
			// 400 before any gate is consulted.
			ServerConfig: &config.WeaviateConfig{Config: config.Config{
				RuntimeReindexEnabled: true,
			}},
			SchemaManager: &schemaUC.Manager{SchemaReader: reader},
			DB:            theDB,
		},
		cluster:        fixedMembership{node},
		backupActivity: prober,
		tasks:          tasks,
	}
}

func submitReindex(h *indexesHandlers) middleware.Responder {
	return submitReindexOn(h, context.Background())
}

// submitReindexOn submits on a context the test controls, which is how a client
// disconnect mid-submission is reproduced.
func submitReindexOn(h *indexesHandlers, ctx context.Context) middleware.Responder {
	return h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
		HTTPRequest:  httptest.NewRequest("PUT", "/", nil).WithContext(ctx),
		ClassName:    "Movies",
		PropertyName: "title",
		Body: &models.IndexUpdateRequest{
			Filterable: &models.IndexUpdateFilterable{Rebuild: true},
		},
	}, &models.Principal{Username: "u1"})
}

// Pins: a backup and a reindex submitted at the same instant can never both
// be admitted, at any point in the submission sequence.
func TestUpdateIndexAdmissionRaceAgainstBackup(t *testing.T) {
	tests := []struct {
		name string
		// claimAt says when the backup takes its slot, relative to the
		// reindex submission.
		claimAt string
	}{
		{name: "backup claims first, before the submission is probed", claimAt: "before"},
		{name: "exact tie: backup claims while the task is being committed", claimAt: "on-commit"},
		{name: "reindex is alone", claimAt: "never"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var busy atomic.Bool
			svc := &raceTaskService{}
			if tc.claimAt == "before" {
				busy.Store(true)
			}
			if tc.claimAt == "on-commit" {
				svc.onCommitted = func() { busy.Store(true) }
			}

			h := submissionHandlers(t, svc, togglingProber{busy: &busy})
			responder := submitReindex(h)

			if tc.claimAt == "never" {
				_, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
				require.Truef(t, ok, "an unopposed reindex must be accepted, got %T", responder)
				require.Empty(t, svc.cancelled, "nothing raced it; the task must stand")
				require.Len(t, svc.startedTasks(), 1)
				return
			}

			conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
			require.Truef(t, ok, "a racing backup must be refused with 409, got %T", responder)
			require.Equal(t,
				"reindex blocked: a backup is running in the cluster; retry after it finishes",
				errorMessage(t, conflict.Payload))

			switch tc.claimAt {
			case "before":
				require.Zero(t, svc.adds, "the backup was already visible; no task should be written")
			case "on-commit":
				require.Equal(t, 1, svc.adds, "the task is written before the race is detectable")
				require.Len(t, svc.cancelled, 1,
					"a refused submission must not leave its task running")
				require.Equal(t, uint64(3), svc.cancelled[0].Version)
			}
			require.Empty(t, svc.startedTasks(),
				"the caller was told the migration did not start; no task may remain STARTED")
		})
	}
}

// Pins: a submission refused by the race must leave nothing behind that
// blocks the retry.
func TestUpdateIndexRefusedByRaceIsRetryable(t *testing.T) {
	var busy atomic.Bool
	svc := &raceTaskService{}
	svc.onCommitted = func() { busy.Store(true) }

	h := submissionHandlers(t, svc, togglingProber{busy: &busy})

	first := submitReindex(h)
	_, refused := first.(*schema.SchemaObjectsIndexesUpdateConflict)
	require.Truef(t, refused, "expected the tie to be refused, got %T", first)
	require.Empty(t, svc.startedTasks())

	svc.onCommitted = nil
	busy.Store(false)

	second := submitReindex(h)
	_, accepted := second.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, accepted,
		"the retry must succeed; the refused attempt left state behind, got %T", second)
	require.Len(t, svc.startedTasks(), 1)
}
