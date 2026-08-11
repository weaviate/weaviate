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

// Shared harness builders and log-inspection helpers for the
// handlers_indexes test files.

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
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

// raceTaskService stands in for the RAFT task service; onCommitted fires where
// the real leader would make the task visible, letting tests place a race there.
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
	// listErr, when set, fails every listing — the shape of an unreachable
	// RAFT leader.
	listErr error
}

func (s *raceTaskService) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	if s.listErr != nil {
		return nil, s.listErr
	}
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

// togglingProber lets a test flip busy at an exact point in the submission
// sequence. An unset kind reports a backup.
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

// cancelFixture builds the cancel handler over one STARTED task whose units
// live on remoteOwner, giving the cancel an owner to wait for.
func cancelFixture(t *testing.T, prober reindexCleanupProber) (*indexesHandlers, *raceTaskService) {
	t.Helper()
	const (
		collection  = "Movies"
		remoteOwner = "node2"
		taskID      = "Movies:repair-filterable:title:ab3f"
	)

	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeRepairFilterable,
		Collection:    collection,
		Properties:    []string{"title"},
		UnitToNode:    map[string]string{"u1": remoteOwner},
		UnitToShard:   map[string]string{"u1": "shard1"},
	})
	require.NoError(t, err)

	svc := &raceTaskService{tasks: []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: taskID, Version: 3},
		Namespace:      db.ReindexNamespace,
		Status:         distributedtask.TaskStatusStarted,
		Payload:        payload,
	}}}

	var busy atomic.Bool
	h := submissionHandlers(t, svc, togglingProber{busy: &busy})
	h.reindexCleanup = prober
	// A real provider, so the gates the cancel closes are the ones a backup
	// would consult.
	h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, h.appState.Logger, "node1",
		func() int { return 1 }, context.Background()))
	return h, svc
}

func errorMessage(t *testing.T, payload *models.ErrorResponse) string {
	t.Helper()
	require.NotNil(t, payload)
	require.Len(t, payload.Error, 1)
	return payload.Error[0].Message
}

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
			// Off makes every submit a 400 before any gate runs.
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

func cancelHandlers(t *testing.T, tasks reindexTaskService) *indexesHandlers {
	t.Helper()
	var busy atomic.Bool
	h := submissionHandlers(t, tasks, togglingProber{busy: &busy})
	h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, h.appState.Logger, fixtureNode,
		func() int { return 1 }, context.Background()))
	return h
}

func submitReindex(h *indexesHandlers) middleware.Responder {
	return submitReindexOn(h, context.Background())
}

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

func gateHandlers(prober reindexCleanupProber, nodes ...string) (*indexesHandlers, *logrustest.Hook) {
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return &indexesHandlers{
		appState:       &state.State{Logger: logger},
		cluster:        fixedMembership(nodes),
		reindexCleanup: prober,
	}, hook
}

func entryWithMessage(hook *logrustest.Hook, fragment string) *logrus.Entry {
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, fragment) {
			return e
		}
	}
	return nil
}

func audited(hook *logrustest.Hook, auditEvent string) *logrus.Entry {
	for _, e := range hook.AllEntries() {
		if e.Data["audit_event"] == auditEvent {
			return e
		}
	}
	return nil
}
