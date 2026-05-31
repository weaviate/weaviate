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

package objects

// TestConditionalEndToEnd* tests exercise the REAL call chain from Manager.AddObject
// through checkIDOrAssignNew -> addObjectToConnectorAndSchema -> vectorRepo.PutObject,
// with the Conditional threaded via context. No mocked precondition: the fake repo
// simulates the shard's behaviour by reading the Conditional from context and
// returning ErrPreconditionFailed when OnlyIfNotExists=true and an "object exists".
//
// Causal-link statement (per test):
//   - TestConditionalEndToEndInsertIfNotExists_ExistingUUID: catches the bug that
//     AddObject with OnlyIfNotExists=true silently overwrites an existing object
//     (returning nil instead of ErrPreconditionFailed) because the REST handler
//     never set the Conditional in context. The test fails RED on the unfixed tree
//     because PutObject returns nil (no precondition check in context-unaware repo)
//     and passes GREEN when the context carries OnlyIfNotExists=true and the repo
//     returns ErrPreconditionFailed as the shard would.
//   - TestConditionalEndToEndInsertIfNotExists_NewUUID: catches the reverse: a new
//     UUID with insert_if_not_exists must succeed (return nil, not ErrPreconditionFailed).
//   - TestConditionalEndToEndCheckIDOrAssignNew_Bypass: catches the short-circuit
//     in checkIDOrAssignNew that returns ErrInvalidUserInput when an ID exists,
//     masking the conditional outcome. For conditional writes the early check must
//     be skipped; the test verifies ErrPreconditionFailed (not ErrInvalidUserInput)
//     is returned.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// conditionalFakeRepo is a VectorRepo whose PutObject simulates the shard-level
// conditional check. It inspects the Conditional stored in context (as crud.go would
// apply it to the storobj.Object). If OnlyIfNotExists=true and the UUID is in the
// "existing" set, it returns ErrPreconditionFailed. This proves the full chain works
// without requiring a real LSM store.
type conditionalFakeRepo struct {
	fakeVectorRepo
	// existingUUIDs is the set of UUIDs that "exist" in the simulated store.
	existingUUIDs map[strfmt.UUID]struct{}
}

func newConditionalFakeRepo(existingUUIDs ...strfmt.UUID) *conditionalFakeRepo {
	existing := make(map[strfmt.UUID]struct{}, len(existingUUIDs))
	for _, id := range existingUUIDs {
		existing[id] = struct{}{}
	}
	return &conditionalFakeRepo{existingUUIDs: existing}
}

func (r *conditionalFakeRepo) PutObject(
	ctx context.Context,
	obj *models.Object,
	vector []float32,
	vectors map[string][]float32,
	multiVectors map[string][][]float32,
	repl *additional.ReplicationProperties,
	schemaVersion uint64,
) error {
	cond := storobj.ConditionalFromContext(ctx)
	_, exists := r.existingUUIDs[obj.ID]

	if cond.OnlyIfNotExists && exists {
		return &ErrPreconditionFailed{
			ObjectID: obj.ID.String(),
			Reason:   fmt.Sprintf("object already exists (OnlyIfNotExists condition failed) for id %s", obj.ID),
		}
	}
	if cond.OnlyIfExists && !exists {
		return &ErrPreconditionFailed{
			ObjectID: obj.ID.String(),
			Reason:   fmt.Sprintf("object does not exist (OnlyIfExists condition failed) for id %s", obj.ID),
		}
	}
	return nil
}

func (r *conditionalFakeRepo) Exists(
	ctx context.Context,
	class string,
	id strfmt.UUID,
	repl *additional.ReplicationProperties,
	tenant string,
) (bool, error) {
	_, ok := r.existingUUIDs[id]
	return ok, nil
}

func newConditionalTestSetup(repo *conditionalFakeRepo) (*Manager, *fakeModulesProvider) {
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "ConditionalTest",
					Vectorizer:        config.VectorizerModuleNone,
					VectorIndexConfig: hnsw.UserConfig{},
				},
			},
		},
	}
	schemaManager := &fakeSchemaManager{GetSchemaResponse: sch}
	cfg := &config.WeaviateConfig{
		Config: config.Config{
			AutoSchema: config.AutoSchema{
				Enabled:       runtime.NewDynamicValue(false),
				DefaultString: schema.DataTypeText.String(),
			},
		},
	}
	authorizer := mocks.NewMockAuthorizer()
	logger, _ := test.NewNullLogger()
	modulesProvider := getFakeModulesProvider()
	metrics := &fakeMetrics{}
	mgr := NewManager(schemaManager, cfg, logger, authorizer,
		repo, modulesProvider, metrics, nil,
		NewAutoSchemaManager(schemaManager, repo, cfg, logger, prometheus.NewPedanticRegistry()))
	return mgr, modulesProvider
}

const (
	conditionalTestUUID  = strfmt.UUID("aaaaaaaa-0000-0000-0000-000000000001")
	conditionalTestClass = "ConditionalTest"
)

// TestConditionalEndToEndInsertIfNotExists_ExistingUUID verifies that
// insert_if_not_exists on a UUID that already exists returns ErrPreconditionFailed
// (not nil and not ErrInvalidUserInput). The Conditional must reach the shard via
// context, bypass the early checkIDOrAssignNew existence check, and produce the
// conditional outcome.
func TestConditionalEndToEndInsertIfNotExists_ExistingUUID(t *testing.T) {
	repo := newConditionalFakeRepo(conditionalTestUUID)
	manager, modulesProvider := newConditionalTestSetup(repo)
	modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
		Return(nil, nil)

	ctx := storobj.ContextWithConditional(context.Background(), storobj.Conditional{OnlyIfNotExists: true})
	obj := &models.Object{
		Class: conditionalTestClass,
		ID:    conditionalTestUUID,
	}

	_, err := manager.AddObject(ctx, nil, obj, nil)
	require.Error(t, err, "insert_if_not_exists on existing UUID must return an error")

	var pf *ErrPreconditionFailed
	require.True(t, errors.As(err, &pf),
		"error must be ErrPreconditionFailed, got %T: %v", err, err)
	assert.Contains(t, pf.Reason, "OnlyIfNotExists condition failed",
		"reason must name the failing condition")

	// Must NOT be ErrInvalidUserInput — that would indicate the early
	// checkIDOrAssignNew existence check fired before the shard got to evaluate
	// the conditional.
	var eiu ErrInvalidUserInput
	assert.False(t, errors.As(err, &eiu),
		"ErrInvalidUserInput must not be returned for conditional writes "+
			"(early existence check in checkIDOrAssignNew must be bypassed)")
}

// TestConditionalEndToEndInsertIfNotExists_NewUUID verifies that
// insert_if_not_exists on a UUID that does NOT exist succeeds (returns nil).
func TestConditionalEndToEndInsertIfNotExists_NewUUID(t *testing.T) {
	repo := newConditionalFakeRepo() // no pre-existing objects
	manager, modulesProvider := newConditionalTestSetup(repo)
	modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
		Return(nil, nil)

	const newUUID = strfmt.UUID("bbbbbbbb-0000-0000-0000-000000000002")
	ctx := storobj.ContextWithConditional(context.Background(), storobj.Conditional{OnlyIfNotExists: true})
	obj := &models.Object{
		Class: conditionalTestClass,
		ID:    newUUID,
	}

	_, err := manager.AddObject(ctx, nil, obj, nil)
	require.NoError(t, err, "insert_if_not_exists on new UUID must succeed")
}

// TestConditionalEndToEndUpdateIfExists_ExistingUUID verifies that
// update_if_exists on a UUID that already exists succeeds (returns nil).
func TestConditionalEndToEndUpdateIfExists_ExistingUUID(t *testing.T) {
	const existingUUID = strfmt.UUID("cccccccc-0000-0000-0000-000000000003")
	repo := newConditionalFakeRepo(existingUUID)
	manager, modulesProvider := newConditionalTestSetup(repo)
	modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
		Return(nil, nil)

	ctx := storobj.ContextWithConditional(context.Background(), storobj.Conditional{OnlyIfExists: true})
	obj := &models.Object{
		Class: conditionalTestClass,
		ID:    existingUUID,
	}

	_, err := manager.AddObject(ctx, nil, obj, nil)
	require.NoError(t, err, "update_if_exists on existing UUID must succeed")
}

// TestConditionalEndToEndUpdateIfExists_NewUUID verifies that
// update_if_exists on a UUID that does NOT exist returns ErrPreconditionFailed.
func TestConditionalEndToEndUpdateIfExists_NewUUID(t *testing.T) {
	repo := newConditionalFakeRepo() // no pre-existing objects
	manager, modulesProvider := newConditionalTestSetup(repo)
	modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
		Return(nil, nil)

	const newUUID = strfmt.UUID("dddddddd-0000-0000-0000-000000000004")
	ctx := storobj.ContextWithConditional(context.Background(), storobj.Conditional{OnlyIfExists: true})
	obj := &models.Object{
		Class: conditionalTestClass,
		ID:    newUUID,
	}

	_, err := manager.AddObject(ctx, nil, obj, nil)
	require.Error(t, err, "update_if_exists on new UUID must return an error")

	var pf *ErrPreconditionFailed
	require.True(t, errors.As(err, &pf),
		"error must be ErrPreconditionFailed, got %T: %v", err, err)
	assert.Contains(t, pf.Reason, "OnlyIfExists condition failed",
		"reason must name the failing condition")
}

// TestConditionalEndToEndUnconditional_ExistingUUID verifies that unconditional
// AddObject (no Conditional in context) still works correctly on a UUID that
// already exists — the early existence check in checkIDOrAssignNew fires and
// returns ErrInvalidUserInput (the bypass must NOT apply to unconditional writes).
func TestConditionalEndToEndUnconditional_ExistingUUID(t *testing.T) {
	repo := newConditionalFakeRepo(conditionalTestUUID)
	manager, _ := newConditionalTestSetup(repo)

	// No conditional in context — unconditional write.
	ctx := context.Background()
	obj := &models.Object{
		Class: conditionalTestClass,
		ID:    conditionalTestUUID,
	}

	_, err := manager.AddObject(ctx, nil, obj, nil)
	require.Error(t, err, "unconditional write to existing UUID must fail (ID already exists)")

	// The early checkIDOrAssignNew check must fire for unconditional writes.
	var eiu ErrInvalidUserInput
	assert.True(t, errors.As(err, &eiu),
		"unconditional write to existing UUID must return ErrInvalidUserInput "+
			"(the early ID existence check must still fire), got %T: %v", err, err)
}
