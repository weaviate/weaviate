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

package db

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema"
)

// Pins OnTaskCompleted's permanent-vs-transient error classification
// (weaviate/0-weaviate-issues#297), asserted via errors.Is, never message substrings.

// classReaderStub implements only ReadOnlyClass; other SchemaReader calls
// panic via the nil embedded interface, surfacing unexpected dependencies.
type classReaderStub struct {
	schema.SchemaReader
	class *models.Class
}

func (r classReaderStub) ReadOnlyClass(string) *models.Class { return r.class }

// flippingReader serves `first` on the first read and `after` on every later one.
type flippingReader struct {
	schema.SchemaReader
	first, after *models.Class
	reads        atomic.Int32
}

func (r *flippingReader) ReadOnlyClass(string) *models.Class {
	if r.reads.Add(1) == 1 {
		return r.first
	}
	return r.after
}

// refusingSchemaManager refuses every migration flip, as the FSM does once the run left SWAPPING.
type refusingSchemaManager struct{ schema.SchemaManager }

func (refusingSchemaManager) UpdatePropertyFromMigration(context.Context, string, api.MigrationRun,
	*models.Property, ...string,
) (uint64, error) {
	return 0, errors.New("no reindex run is completing; its cutover write is refused")
}

// newClassificationProvider wires a ReindexProvider with just the schema
// reader the flip path reads; cls=nil models a class that's not locally readable.
func newClassificationProvider(cls *models.Class) *ReindexProvider {
	logger, _ := test.NewNullLogger()
	return &ReindexProvider{
		logger:        logger,
		schemaManager: &schema.Manager{SchemaReader: classReaderStub{class: cls}},
		payloads:      make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
		reindexTasks:  make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric),
		endedTasks:    make(map[distributedtask.TaskDescriptor]struct{}),
	}
}

func TestOnTaskCompletedClassification(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	t.Run("unparsable payload is permanent", func(t *testing.T) {
		// Never becomes parsable, so this must be permanent, not transient.
		p := &ReindexProvider{
			logger:       logger,
			payloads:     make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
			reindexTasks: make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric),
			endedTasks:   make(map[distributedtask.TaskDescriptor]struct{}),
		}
		task := &distributedtask.Task{
			Namespace:      "reindex",
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
			Status:         distributedtask.TaskStatusSwapping,
			Payload:        []byte("{ this is not valid json"),
		}
		err := p.OnTaskCompleted(task)
		require.Error(t, err)
		require.ErrorIs(t, err, distributedtask.ErrTaskCompletionPermanent)
	})

	t.Run("change-tokenization target property gone is permanent", func(t *testing.T) {
		// Property deleted between submit and finalize can never be
		// flipped — permanent, not transient.
		p := newClassificationProvider(&models.Class{Class: "Docs"})
		payload := &ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenization,
			Collection:         "Docs",
			Properties:         []string{"title"},
			TargetTokenization: models.PropertyTokenizationWord,
		}
		err := p.flipSemanticMigrationSchema(ctx, api.MigrationRun{}, payload, logger)
		require.Error(t, err)
		require.ErrorIs(t, err, distributedtask.ErrTaskCompletionPermanent)
	})

	t.Run("change-tokenization non-missing flip error is transient", func(t *testing.T) {
		// Any non-property-gone flip error (here: class not locally
		// readable) must stay plain, or the scheduler would FAIL a recoverable migration.
		p := newClassificationProvider(nil)
		payload := &ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenization,
			Collection:         "Docs",
			Properties:         []string{"title"},
			TargetTokenization: models.PropertyTokenizationWord,
		}
		err := p.flipSemanticMigrationSchema(ctx, api.MigrationRun{}, payload, logger)
		require.Error(t, err)
		require.NotErrorIs(t, err, distributedtask.ErrTaskCompletionPermanent)
	})

	t.Run("enable-filterable tolerates a missing property", func(t *testing.T) {
		// enable-* treat a dropped property as already-filterable, not an error.
		p := newClassificationProvider(&models.Class{Class: "Docs"})
		payload := &ReindexTaskPayload{
			MigrationType: ReindexTypeEnableFilterable,
			Collection:    "Docs",
			Properties:    []string{"title"},
		}
		err := p.flipSemanticMigrationSchema(ctx, api.MigrationRun{}, payload, logger)
		require.NoError(t, err)
	})

	t.Run("enable-rangeable flips from here, not from the first shard to swap", func(t *testing.T) {
		// Missing property tolerated like the other enable-* types; an
		// already-true flag is skipped, so N nodes produce one RAFT commit.
		trueVal := true
		for _, tc := range []struct {
			name  string
			props []*models.Property
		}{
			{name: "property gone", props: nil},
			{
				name:  "flag already true",
				props: []*models.Property{{Name: "price", IndexRangeFilters: &trueVal}},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				p := newClassificationProvider(&models.Class{Class: "Docs", Properties: tc.props})
				payload := &ReindexTaskPayload{
					MigrationType: ReindexTypeEnableRangeable,
					Collection:    "Docs",
					Properties:    []string{"price"},
				}
				require.NoError(t, p.flipSemanticMigrationSchema(ctx, api.MigrationRun{}, payload, logger))
			})
		}
	})

	t.Run("only a swapping task reaches the flip", func(t *testing.T) {
		// A flip on any status but swapping would commit a cluster-wide
		// schema change for a migration whose buckets were never swapped.
		payloadJSON, err := json.Marshal(ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenization,
			Collection:         "Docs",
			Properties:         []string{"title"},
			TargetTokenization: models.PropertyTokenizationWord,
		})
		require.NoError(t, err)

		for _, tc := range []struct {
			name     string
			status   distributedtask.TaskStatus
			wantFlip bool
		}{
			{name: "finished", status: distributedtask.TaskStatusFinished},
			{name: "swapping", status: distributedtask.TaskStatusSwapping, wantFlip: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// The class is not locally readable, so a flip that runs
				// errors and one that never runs cannot.
				p := newClassificationProvider(nil)
				err := p.OnTaskCompleted(&distributedtask.Task{
					Namespace:      ReindexNamespace,
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "t2", Version: 1},
					Status:         tc.status,
					Payload:        payloadJSON,
				})
				if tc.wantFlip {
					require.Error(t, err, "swapping is the status that owns the cluster-wide flip")
					return
				}
				require.NoError(t, err, "a non-swapping status must not touch the schema")
			})
		}
	})

	t.Run("a refused flip is done when the schema already shows the target", func(t *testing.T) {
		// Another node's flip landed and finalized the run, so ours is refused; retrying or
		// failing the run would act on a migration that already succeeded.
		payload := ReindexTaskPayload{
			MigrationType: ReindexTypeChangeTokenization, Collection: "Docs",
			Properties: []string{"title"}, TargetTokenization: models.PropertyTokenizationField,
		}
		payloadJSON, err := json.Marshal(payload)
		require.NoError(t, err)
		task := &distributedtask.Task{
			Namespace:      ReindexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "t3", Version: 1},
			Status:         distributedtask.TaskStatusSwapping,
			Payload:        payloadJSON,
		}
		classWith := func(tok string) *models.Class {
			return &models.Class{Class: "Docs", Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}, Tokenization: tok},
			}}
		}

		for _, tc := range []struct {
			name    string
			after   string
			wantErr bool
		}{
			{name: "schema flipped meanwhile", after: models.PropertyTokenizationField},
			{name: "schema still pre-flip", after: models.PropertyTokenizationWord, wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// The first read is the stale one the flip is built from; the refusal comes after the flip landed.
				reader := &flippingReader{first: classWith(models.PropertyTokenizationWord), after: classWith(tc.after)}
				handler, err := schema.NewHandler(reader, refusingSchemaManager{}, nil, logger, nil, nil,
					config.Config{}, nil, nil, nil, nil, nil, nil, schema.Parser{}, nil, nil, nil)
				require.NoError(t, err)
				p := newClassificationProvider(nil)
				p.schemaManager = &schema.Manager{SchemaReader: reader, Handler: handler}

				err = p.OnTaskCompleted(task)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
			})
		}
	})

	t.Run("change-algorithm finishes once the property is stamped, even if the class flag cannot flip", func(t *testing.T) {
		// The class flip is refused, here by a replication minimum raised after the class was created.
		trueVal := true
		cls := &models.Class{
			Class: "Docs", InvertedIndexConfig: &models.InvertedIndexConfig{},
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			Properties: []*models.Property{{
				Name: "title", DataType: []string{"text"}, IndexSearchable: &trueVal, SearchableBlockmax: &trueVal,
			}},
		}
		reader := classReaderStub{class: cls}
		handler, err := schema.NewHandler(reader, nil, nil, logger, nil, nil,
			config.Config{Replication: replication.GlobalConfig{MinimumFactor: 3}},
			nil, nil, nil, nil, nil, nil, schema.Parser{}, nil, nil, nil)
		require.NoError(t, err)
		p := newClassificationProvider(cls)
		p.schemaManager = &schema.Manager{SchemaReader: reader, Handler: handler}
		payload := &ReindexTaskPayload{
			MigrationType: ReindexTypeChangeAlgorithm, Collection: "Docs", Properties: []string{"title"},
		}
		require.NoError(t, p.flipSemanticMigrationSchema(ctx, api.MigrationRun{}, payload, logger),
			"a failed run would lock the migrated property behind a run that cannot finish")
	})

	t.Run("enable-searchable tolerates a missing property", func(t *testing.T) {
		p := newClassificationProvider(&models.Class{Class: "Docs"})
		payload := &ReindexTaskPayload{
			MigrationType:      ReindexTypeEnableSearchable,
			Collection:         "Docs",
			Properties:         []string{"title"},
			TargetTokenization: models.PropertyTokenizationWord,
		}
		err := p.flipSemanticMigrationSchema(ctx, api.MigrationRun{}, payload, logger)
		require.NoError(t, err)
	})
}
