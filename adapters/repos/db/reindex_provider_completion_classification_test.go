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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
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

// newClassificationProvider wires a ReindexProvider with just the schema
// reader the flip path reads; cls=nil models a class that's not locally readable.
func newClassificationProvider(cls *models.Class) *ReindexProvider {
	logger, _ := test.NewNullLogger()
	return &ReindexProvider{
		logger:        logger,
		schemaManager: &schema.Manager{SchemaReader: classReaderStub{class: cls}},
		payloads:      make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
		reindexTasks:  make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric),
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
		err := p.flipSemanticMigrationSchema(ctx, payload, logger)
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
		err := p.flipSemanticMigrationSchema(ctx, payload, logger)
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
		err := p.flipSemanticMigrationSchema(ctx, payload, logger)
		require.NoError(t, err)
	})

	t.Run("enable-rangeable tolerates a missing property", func(t *testing.T) {
		// Without the rangeable arm this returns "unexpected semantic
		// migration type" and every converted task dies at completion.
		p := newClassificationProvider(&models.Class{Class: "Docs"})
		payload := &ReindexTaskPayload{
			MigrationType: ReindexTypeEnableRangeable,
			Collection:    "Docs",
			Properties:    []string{"price"},
		}
		require.NoError(t, p.flipSemanticMigrationSchema(ctx, payload, logger))
	})

	t.Run("rangeable flip is a no-op when the flag is already true", func(t *testing.T) {
		// The mutator returns apply=false, so no RAFT command is built —
		// which is why repair-rangeable (flag true at submit) can share
		// the arm. A commit attempt would panic on the stub's nil Handler.
		trueVal := true
		cls := &models.Class{
			Class: "Docs",
			Properties: []*models.Property{
				{Name: "price", IndexRangeFilters: &trueVal},
			},
		}
		for _, mt := range []ReindexMigrationType{ReindexTypeEnableRangeable, ReindexTypeRepairRangeable} {
			p := newClassificationProvider(cls)
			payload := &ReindexTaskPayload{
				MigrationType: mt,
				Collection:    "Docs",
				Properties:    []string{"price"},
			}
			require.NoError(t, p.flipSemanticMigrationSchema(ctx, payload, logger), mt)
		}
	})

	t.Run("enable-searchable tolerates a missing property", func(t *testing.T) {
		p := newClassificationProvider(&models.Class{Class: "Docs"})
		payload := &ReindexTaskPayload{
			MigrationType:      ReindexTypeEnableSearchable,
			Collection:         "Docs",
			Properties:         []string{"title"},
			TargetTokenization: models.PropertyTokenizationWord,
		}
		err := p.flipSemanticMigrationSchema(ctx, payload, logger)
		require.NoError(t, err)
	})
}
