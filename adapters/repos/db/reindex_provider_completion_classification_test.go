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

// The reindex provider is the PRODUCER of the permanent-vs-transient
// decision the scheduler acts on: a permanent failure (wrapped in
// [distributedtask.ErrTaskCompletionPermanent]) FAILs the task
// immediately, a transient one is retried up to the per-task bound. A
// transient failure mislabeled permanent would prematurely FAIL a
// recoverable migration; the inverse would spin the retry budget on a
// call that can never succeed. Both classes are pinned here against the
// real classification code, on the sentinel via errors.Is — never on
// message substrings.

// classReaderStub supplies only ReadOnlyClass, the single schema
// dependency the finalize classification consults on the missing-property
// and unreadable-class paths. Every other SchemaReader method is promoted
// from the nil embedded interface: a call would panic, surfacing an
// unexpected dependency instead of passing silently.
type classReaderStub struct {
	schema.SchemaReader
	class *models.Class
}

func (r classReaderStub) ReadOnlyClass(string) *models.Class { return r.class }

// newClassificationProvider builds a ReindexProvider wired with just the
// schema reader the flip path reads from. ReadOnlyClass returns cls (nil
// models a class that is not locally readable). The Manager's Handler is
// left zero-valued: the missing-property and unreadable-class paths never
// reach the RAFT UpdateProperty call, so no FSM wiring is needed.
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
		// A payload that no longer parses can never parse on retry, so
		// OnTaskCompleted classifies it permanent up front rather than
		// re-firing the flip forever.
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
		// The class survives but the target property was deleted between
		// submit and finalize: applyPerPropertySchemaUpdate reports it in
		// missing, and a single-property tokenization flip against an
		// absent property can never commit.
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
		// A flip error that is NOT the property-gone case (here the class
		// is not locally readable, standing in for any RAFT/apply error)
		// flows through the shared `if err != nil` return in
		// flipSemanticMigrationSchema — the exact line a RAFT apply error
		// takes. It must stay plain so the scheduler retries within its
		// bound instead of FAILing a recoverable migration.
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
		// enable-* discard the missing list — a dropped property is the
		// same end state as "already filterable". A missing property must
		// not surface as any error, let alone permanent.
		p := newClassificationProvider(&models.Class{Class: "Docs"})
		payload := &ReindexTaskPayload{
			MigrationType: ReindexTypeEnableFilterable,
			Collection:    "Docs",
			Properties:    []string{"title"},
		}
		err := p.flipSemanticMigrationSchema(ctx, payload, logger)
		require.NoError(t, err)
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
