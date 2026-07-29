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

package reindex_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	schemauc "github.com/weaviate/weaviate/usecases/schema"
)

// stampCall records one masked-RAFT UpdatePropertyFromMigration the repair fires.
type stampCall struct {
	class  string
	prop   string
	stamp  *bool
	fields []string
}

// capturingSchemaManager records the stamp write (UpdatePropertyFromMigration)
// and no-ops every other SchemaManager method via the nil embedded interface,
// so an unexpected schema dependency surfaces as a nil-pointer panic instead of
// silently passing.
type capturingSchemaManager struct {
	schemauc.SchemaManager
	mu     sync.Mutex
	stamps []stampCall
}

func (c *capturingSchemaManager) UpdatePropertyFromMigration(_ context.Context, class string, prop *models.Property, fields ...string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stamps = append(c.stamps, stampCall{class: class, prop: prop.Name, stamp: prop.SearchableBlockmax, fields: fields})
	return 1, nil
}

func (c *capturingSchemaManager) recorded() []stampCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]stampCall(nil), c.stamps...)
}

// repairResidualReader returns the residual class for ReadOnlyClass /
// ReadOnlySchema and no-ops WaitForUpdate (the local-apply wait after the
// stamp write). Every other SchemaReader call panics via the nil embed.
type repairResidualReader struct {
	schemauc.SchemaReader
	class *models.Class
}

func (r repairResidualReader) ReadOnlyClass(string) *models.Class          { return r.class }
func (r repairResidualReader) WaitForUpdate(context.Context, uint64) error { return nil }

// ReadOnlySchema backs the per-pass class enumeration in
// reconcileSearchableBlockmaxStamps (via Handler.GetSchemaSkipAuth).
func (r repairResidualReader) ReadOnlySchema() models.Schema {
	return models.Schema{Classes: []*models.Class{r.class}}
}

// blockmaxRepairDBStub is the reindex.DBLike the repair consults for on-disk
// evidence. A zero-value stub (nil idx) models a node holding no shard for the
// class — db.GetIndex returns nil there, so the FINISHED task is the sole
// seeding evidence. Every other DBLike method panics via the nil embed.
type blockmaxRepairDBStub struct {
	reindex.DBLike
	className string
	idx       reindex.IndexLike
}

func (s blockmaxRepairDBStub) GetIndex(className entschema.ClassName) reindex.IndexLike {
	if string(className) != s.className {
		return nil
	}
	return s.idx
}

// blockmaxRepairTaskLister feeds the repair its FINISHED-task evidence and
// doubles as the RAFT-readiness probe RunSearchableBlockmaxRepair waits on.
type blockmaxRepairTaskLister struct{ tasks []*distributedtask.Task }

func (l blockmaxRepairTaskLister) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return map[string][]*distributedtask.Task{reindex.ReindexNamespace: l.tasks}, nil
}

// runSearchableBlockmaxRepairOnce drives exactly one read-repair pass through
// the exported entry point. reconcileClassSearchableBlockmax is unexported and
// this file must live in the external test package (it drives a real shard), so
// RunSearchableBlockmaxRepair is the reachable seam.
//
// The task-lister stub answers the readiness probe on the first attempt, so the
// pass runs immediately; the call then parks in the 5-minute interval select
// until the deadline context expires and it returns. One pass, no truncation.
func runSearchableBlockmaxRepairOnce(t *testing.T, p *reindex.ReindexProvider) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.RunSearchableBlockmaxRepair(ctx)
}

// newBlockmaxRepairProvider wires a provider whose only live dependencies are
// the schema fakes, the on-disk handle and the task list.
func newBlockmaxRepairProvider(t *testing.T, residualClass *models.Class,
	dbStub reindex.DBLike, tasks []*distributedtask.Task,
) (*reindex.ReindexProvider, *capturingSchemaManager) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	capMgr := &capturingSchemaManager{}
	reader := repairResidualReader{class: residualClass}
	// Real schema.Manager wired to fakes: the stamp write routes through
	// Handler's unexported schemaManager/schemaReader, so NewHandler is the
	// only way to inject the capture; mgr.ReadOnlyClass resolves via the embedded SchemaReader.
	h, err := schemauc.NewHandler(reader, capMgr, nil, logger, nil, nil, config.Config{},
		nil, nil, nil, nil, nil, nil, schemauc.Parser{}, nil, nil, nil)
	require.NoError(t, err)
	mgr := &schemauc.Manager{Handler: h, SchemaReader: reader}

	p := reindex.NewReindexProvider(dbStub, mgr, blockmaxRepairTaskLister{tasks: tasks},
		logger, "node1", nil, context.Background())
	return p, capMgr
}

// TestReconcileClassSearchableBlockmax_BackfillsResidualStamp pins the
// v1.38→v1.39 read-repair: a nil-stamp property genuinely blockmax on disk, in
// a permanently-partial class with no live task, resolves as WAND until the
// repair observes the on-disk StrategyInverted bucket and seeds the stamp —
// only for the prop actually blockmax on disk.
func TestReconcileClassSearchableBlockmax_BackfillsResidualStamp(t *testing.T) {
	ctx := testCtx()
	className := "BlockmaxRepairResidual"
	tr := true

	// Build the on-disk residual directly: propX stamped blockmax gets a
	// StrategyInverted bucket, propY (unstamped, partial class) gets
	// StrategyMapCollection — the per-prop override in shard_init_properties.go
	// reproduces exactly what a real partial-class migration leaves on disk.
	initClass := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false, // permanently-partial class
		},
		Properties: []*models.Property{
			{Name: "blockmaxprop", DataType: entschema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord, SearchableBlockmax: &tr},
			{Name: "wandprop", DataType: entschema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord},
		},
	}

	shd, idx, _ := testShardWithSettings(t, ctx, initClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)
	defer shard.Shutdown(ctx)

	// Precondition: on-disk searchable bucket strategies diverge as intended.
	require.Equal(t, lsmkv.StrategyInverted,
		shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM("blockmaxprop")).Strategy(),
		"blockmaxprop must be blockmax (StrategyInverted) on disk")
	require.Equal(t, lsmkv.StrategyMapCollection,
		shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM("wandprop")).Strategy(),
		"wandprop must be WAND (StrategyMapCollection) on disk")

	// The residual as RAFT sees it after the pre-stamp upgrade: both stamps nil,
	// class flag false, no live task. blockmaxprop mis-resolves as WAND — the
	// exact bug the backfill closes.
	residualClass := &models.Class{
		Class:               className,
		InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: false},
		Properties: []*models.Property{
			{Name: "blockmaxprop", DataType: entschema.DataTypeText.PropString()},
			{Name: "wandprop", DataType: entschema.DataTypeText.PropString()},
		},
	}
	require.False(t, reindex.SearchablePropertyIsBlockmax(residualClass, "blockmaxprop", nil),
		"pre-repair: nil stamp + false class flag + no task → resolver reads blockmax prop back as WAND")

	// nil tasks: this test isolates the on-disk seeding source.
	p, capMgr := newBlockmaxRepairProvider(t, residualClass,
		blockmaxRepairDBStub{className: className, idx: idx.ReindexHandle()}, nil)

	runSearchableBlockmaxRepairOnce(t, p)

	// Exactly one stamp fired, for the blockmax-on-disk prop, set to true. The
	// WAND prop shares the nil-stamp candidate condition but is NOT stamped
	// because its on-disk bucket is StrategyMapCollection.
	stamps := capMgr.recorded()
	require.Len(t, stamps, 1, "repair must seed exactly the blockmax-on-disk residual prop")
	require.Equal(t, "blockmaxprop", stamps[0].prop)
	require.NotNil(t, stamps[0].stamp)
	require.True(t, *stamps[0].stamp, "seeded stamp must be true")
	require.Contains(t, stamps[0].fields, api.PropertyFieldSearchableBlockmax,
		"stamp write must use the searchableBlockmax fieldmask")

	// The seeded stamp (searchableBlockmax=true) is what flips the resolver from
	// WAND to blockmax — closing the residual.
	stampedClass := &models.Class{
		Class:               className,
		InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: false},
		Properties: []*models.Property{
			{Name: "blockmaxprop", DataType: entschema.DataTypeText.PropString(), SearchableBlockmax: &tr},
		},
	}
	require.True(t, reindex.SearchablePropertyIsBlockmax(stampedClass, "blockmaxprop", nil),
		"post-repair: with the seeded stamp the resolver reads blockmax")
}

// TestReconcileClassSearchableBlockmax_SeedsFromFinishedTaskWhileShardless
// pins that a nil-stamp property with no local shard is still seeded from a
// FINISHED blockmax-producing task before it ages out; a prop with no
// FINISHED task is left untouched.
func TestReconcileClassSearchableBlockmax_SeedsFromFinishedTaskWhileShardless(t *testing.T) {
	className := "BlockmaxRepairFinishedTask"

	residualClass := &models.Class{
		Class:               className,
		InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: false},
		Properties: []*models.Property{
			{Name: "blockmaxprop", DataType: entschema.DataTypeText.PropString()},
			{Name: "wandprop", DataType: entschema.DataTypeText.PropString()},
		},
	}

	// A FINISHED change-algorithm (blockmax-producing) task for blockmaxprop
	// only; wandprop has no completed migration.
	payload, err := json.Marshal(reindex.ReindexTaskPayload{
		Collection: className, MigrationType: reindex.ReindexTypeChangeAlgorithm, Properties: []string{"blockmaxprop"},
	})
	require.NoError(t, err)
	tasks := []*distributedtask.Task{{Status: distributedtask.TaskStatusFinished, Payload: payload}}

	// Shardless for this class: db.GetIndex returns nil, so there is no on-disk
	// observation — the FINISHED task is the sole seeding evidence.
	p, capMgr := newBlockmaxRepairProvider(t, residualClass, blockmaxRepairDBStub{}, tasks)

	runSearchableBlockmaxRepairOnce(t, p)

	stamps := capMgr.recorded()
	require.Len(t, stamps, 1, "only the prop with a FINISHED blockmax task is seeded")
	require.Equal(t, "blockmaxprop", stamps[0].prop)
	require.NotNil(t, stamps[0].stamp)
	require.True(t, *stamps[0].stamp, "seeded stamp must be true")
}
