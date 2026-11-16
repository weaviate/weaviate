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
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	nullLengthObjects = 24
	swapValue         = "value"
	swapNull          = "EnableNullState"
	swapLength        = "EnablePropertyLength"
	restartStep       = "restart"
	// The run finished, and the schema flip is still to come.
	restartFinishedStep = "restart once finished"
)

type nullLengthEnable struct {
	name string
	mt   ReindexMigrationType
	dt   schema.DataType
	off  func(p *models.Property)
	on   func(p *models.Property)
}

func nullLengthEnables() []nullLengthEnable {
	f, tr := false, true
	return []nullLengthEnable{
		{
			name: "enable-filterable", mt: ReindexTypeEnableFilterable, dt: schema.DataTypeText,
			off: func(p *models.Property) { p.IndexFilterable, p.IndexSearchable = &f, &f },
			on:  func(p *models.Property) { p.IndexFilterable = &tr },
		},
		{
			name: "enable-searchable", mt: ReindexTypeEnableSearchable, dt: schema.DataTypeText,
			off: func(p *models.Property) { p.IndexFilterable, p.IndexSearchable = &f, &f },
			on:  func(p *models.Property) { p.IndexSearchable = &tr },
		},
		{
			name: "enable-rangeable", mt: ReindexTypeEnableRangeable, dt: schema.DataTypeInt,
			off: func(p *models.Property) { p.IndexFilterable = &f },
			on:  func(p *models.Property) { p.IndexRangeFilters = &tr },
		},
	}
}

type nullLengthScenario struct {
	enable       nullLengthEnable
	null, length bool
	// q is a second payload property that already has an index.
	withIndexedQ bool
	steps        []string
}

func (sc nullLengthScenario) class(name string, enabled bool) *models.Class {
	class := newTestClassWithProps(name, []string{"p", "q", "other"})
	class.InvertedIndexConfig.IndexNullState = sc.null
	class.InvertedIndexConfig.IndexPropertyLength = sc.length
	p := class.Properties[0]
	p.DataType = sc.enable.dt.PropString()
	if sc.enable.dt != schema.DataTypeText {
		p.Tokenization = ""
	}
	sc.enable.off(p)
	if enabled {
		sc.enable.on(p)
		if sc.withIndexedQ {
			sc.enable.on(class.Properties[1])
		}
	}
	return class
}

func (sc nullLengthScenario) payloadProps() []string {
	if sc.withIndexedQ {
		return []string{"p", "q"}
	}
	return []string{"p"}
}

// variant 0 leaves p out, 1 sets it empty, 2 and up give it a value.
func nullLengthObject(className string, id, variant int, dt schema.DataType) *storobj.Object {
	props := map[string]any{"other": "x", "q": []string{"", "b", "b c"}[variant%3]}
	switch {
	case variant == 0:
	case dt == schema.DataTypeInt:
		props["p"] = int64(variant)
	case variant == 1:
		props["p"] = ""
	default:
		props["p"] = []string{"a", "a b", "a b c"}[variant%3]
	}
	return &storobj.Object{MarshallerVersion: 1, Object: models.Object{
		ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", id)),
		Class: className, Properties: props,
	}}
}

func buildEnableUnitTasks(logger logrus.FieldLogger, version uint64, payload *ReindexTaskPayload,
	class *models.Class,
) ([]*ShardReindexTaskGeneric, error) {
	return buildMigrationTasks(logger, nil, version, payload, func(string, string) bool { return true },
		func(_, prop string) (bool, bool) { return nullLengthCompanionsIn(class, prop) })
}

type enableRun struct {
	payload *ReindexTaskPayload
	tasks   []*ShardReindexTaskGeneric
}

// newEnableRun builds the unit's tasks the way the provider does, answering
// the companion question from the class before the run.
func newEnableRun(t *testing.T, shard *Shard, class *models.Class, mt ReindexMigrationType, props []string,
	version uint64,
) *enableRun {
	t.Helper()
	payload := &ReindexTaskPayload{MigrationType: mt, Collection: class.Class, Properties: props}
	if mt == ReindexTypeEnableSearchable {
		payload.TargetTokenization = models.PropertyTokenizationWord
	}
	before := *class
	before.Properties = slices.Clone(class.Properties)
	for i, p := range before.Properties {
		cp := *p
		before.Properties[i] = &cp
	}
	tasks, err := buildEnableUnitTasks(shard.index.logger, version, payload, &before)
	require.NoError(t, err)
	for _, task := range tasks {
		task.setMigrationIdentity(distributedtask.TaskDescriptor{ID: "enable-" + class.Class, Version: version},
			shard.migrationUnit(), payload)
	}
	return &enableRun{payload: payload, tasks: tasks}
}

func (r *enableRun) all(t *testing.T, ctx context.Context, shard *Shard) {
	t.Helper()
	r.each(t, func(task *ShardReindexTaskGeneric) error { return task.RunReindexOnlyOnShard(ctx, shard) })
	r.each(t, func(task *ShardReindexTaskGeneric) error { return task.RunPrepareOnShard(ctx, shard) })
	r.swap(t, ctx, shard, swapValue, swapNull, swapLength)
}

func (r *enableRun) each(t *testing.T, fn func(*ShardReindexTaskGeneric) error) {
	t.Helper()
	for _, task := range r.tasks {
		require.NoError(t, fn(task), task.Name())
	}
}

// swap runs the provider's swap phase on the named tasks only; a name the
// run did not build is skipped.
func (r *enableRun) swap(t *testing.T, ctx context.Context, shard *Shard, names ...string) {
	t.Helper()
	var subset []*ShardReindexTaskGeneric
	for _, task := range r.tasks {
		name := task.Name()
		if !slices.Contains([]string{swapNull, swapLength}, name) {
			name = swapValue
		}
		if slices.Contains(names, name) {
			subset = append(subset, task)
		}
	}
	if len(subset) == 0 {
		return
	}
	logger, _ := logrustest.NewNullLogger()
	p := &ReindexProvider{logger: logger, localNode: "node1", serverCtx: ctx}
	res := p.runShardSwapPhase(ctx, r.payload, shard.migrationUnit(), shard.Name(), shard, subset, logger)
	require.Empty(t, res.Errs)
}

func restartForRun(t *testing.T, ctx context.Context, idx *Index, shard *Shard, class *models.Class,
	status distributedtask.TaskStatus,
) *Shard {
	t.Helper()
	name := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())
	idx.db.indices[indexID(idx.Config.ClassName)] = idx
	installTestMigrationTaskSources(ctx, idx.db, nil, &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "enable-" + class.Class, Version: 1},
		Status:         status,
	})
	provider := NewReindexProvider(idx.db, nil, nil, idx.logger, idx.getSchema.NodeName(), nil, ctx)
	idx.shardReindexer = NewMigrationRecoveryReindexer(idx.logger, nil, provider)
	return openShardFromDisk(t, ctx, idx, class, name)
}

func nullLengthCount(t *testing.T, ctx context.Context, s *Shard, className, prop string, op filters.Operator, v any) int {
	t.Helper()
	dt := schema.DataTypeBoolean
	if op != filters.OperatorIsNull {
		dt = schema.DataTypeInt
	}
	res, _, err := s.ObjectSearch(ctx, 1000, &filters.LocalFilter{Root: &filters.Clause{
		Operator: op, Value: &filters.Value{Value: v, Type: dt},
		On: &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(prop)},
	}}, nil, nil, nil, additional.Properties{}, nil)
	require.NoError(t, err)
	return len(res)
}

func requireSameNullLength(t *testing.T, ctx context.Context, sc nullLengthScenario,
	ctrl, got *Shard, ctrlClass, gotClass, when string,
) {
	t.Helper()
	for _, prop := range sc.payloadProps() {
		for _, bucket := range []string{helpers.BucketFromPropNameNullLSM(prop), helpers.BucketFromPropNameLengthLSM(prop)} {
			want := fingerprintRoaringSetBucket(t, ctrl.store.Bucket(bucket))
			require.Equal(t, want, fingerprintRoaringSetBucket(t, got.store.Bucket(bucket)), "%s: %s", when, bucket)
		}
	}
	queries := map[string]func(*Shard, string) int{}
	if sc.null {
		queries["p is null"] = func(s *Shard, c string) int {
			return nullLengthCount(t, ctx, s, c, "p", filters.OperatorIsNull, true)
		}
		queries["p is not null"] = func(s *Shard, c string) int {
			return nullLengthCount(t, ctx, s, c, "p", filters.OperatorIsNull, false)
		}
	}
	if sc.length && sc.enable.dt == schema.DataTypeText {
		queries["len(p) = 0"] = func(s *Shard, c string) int {
			return nullLengthCount(t, ctx, s, c, "len(p)", filters.OperatorEqual, 0)
		}
		queries["len(p) > 1"] = func(s *Shard, c string) int {
			return nullLengthCount(t, ctx, s, c, "len(p)", filters.OperatorGreaterThan, 1)
		}
	}
	for name, count := range queries {
		want := count(ctrl, ctrlClass)
		require.NotZero(t, want, "%s: the control must match something for %q", when, name)
		require.Equal(t, want, count(got, gotClass), "%s: %s", when, name)
	}
}

// nullLengthFixture applies every write to the migrating shard and to a
// control that had the index from the start, so the two have to match.
type nullLengthFixture struct {
	t                *testing.T
	ctx              context.Context
	sc               nullLengthScenario
	ctrl, shard      *Shard
	ctrlClass, class *models.Class
	idx              *Index
	nextID           int
}

func newNullLengthFixture(t *testing.T, sc nullLengthScenario) *nullLengthFixture {
	f := &nullLengthFixture{t: t, ctx: testCtx(), sc: sc, nextID: nullLengthObjects}
	suffix := uuid.NewString()[:8]
	f.ctrlClass = sc.class("CtrlNL_"+suffix, true)
	ctrl, _ := testShardWithSettings(t, f.ctx, f.ctrlClass, enthnsw.UserConfig{Skip: true}, false, false, false)
	f.ctrl = ctrl.(*Shard)
	f.class = sc.class("MigNL_"+suffix, false)
	shard, idx := testShardWithSettings(t, f.ctx, f.class, enthnsw.UserConfig{Skip: true}, false, false, false)
	f.shard, f.idx = shard.(*Shard), idx
	t.Cleanup(func() {
		f.ctrl.Shutdown(context.Background())
		f.shard.Shutdown(context.Background())
	})
	for i := 0; i < nullLengthObjects; i++ {
		f.put(i, i%4)
	}
	return f
}

func (f *nullLengthFixture) put(id, variant int) {
	require.NoError(f.t, f.ctrl.PutObject(f.ctx, nullLengthObject(f.ctrlClass.Class, id, variant, f.sc.enable.dt)))
	require.NoError(f.t, f.shard.PutObject(f.ctx, nullLengthObject(f.class.Class, id, variant, f.sc.enable.dt)))
}

func (f *nullLengthFixture) write(step int) {
	f.put(f.nextID, step%3+1)
	f.put(f.nextID+1, 0)
	f.nextID += 2
	f.put(step, step%4)   // an earlier object to absent, empty or a value
	f.put(step+6, step+2) // another one to a value
	deleted := nullLengthObject("", step+12, 0, f.sc.enable.dt).ID()
	require.NoError(f.t, f.ctrl.DeleteObject(f.ctx, deleted, time.Now()))
	require.NoError(f.t, f.shard.DeleteObject(f.ctx, deleted, time.Now()))
}

func (f *nullLengthFixture) run(version uint64) *enableRun {
	return newEnableRun(f.t, f.shard, f.class, f.sc.enable.mt, f.sc.payloadProps(), version)
}

// setIndex flips the payload properties' index on, or drops every index.
func (f *nullLengthFixture) setIndex(on bool) {
	off := false
	for i := range f.sc.payloadProps() {
		prop := f.class.Properties[i]
		if on {
			f.sc.enable.on(prop)
		} else {
			prop.IndexFilterable, prop.IndexSearchable, prop.IndexRangeFilters = &off, &off, &off
		}
		require.NoError(f.t, f.idx.updateProperty(f.ctx, prop))
	}
}

func (f *nullLengthFixture) requireMatch(when string) {
	requireSameNullLength(f.t, f.ctx, f.sc, f.ctrl, f.shard, f.ctrlClass.Class, f.class.Class, when)
}

func runNullLengthScenario(t *testing.T, sc nullLengthScenario) {
	f := newNullLengthFixture(t, sc)
	run := f.run(1)
	run.each(t, func(task *ShardReindexTaskGeneric) error { return task.RunReindexOnlyOnShard(f.ctx, f.shard) })
	f.write(1)
	run.each(t, func(task *ShardReindexTaskGeneric) error { return task.RunPrepareOnShard(f.ctx, f.shard) })
	f.write(2)
	for i, step := range sc.steps {
		switch step {
		case restartStep:
			f.shard = restartForRun(t, f.ctx, f.idx, f.shard, f.class, distributedtask.TaskStatusSwapping)
			run = f.run(1)
		case restartFinishedStep:
			f.shard = restartForRun(t, f.ctx, f.idx, f.shard, f.class, distributedtask.TaskStatusFinished)
			run = f.run(1)
		default:
			run.swap(t, f.ctx, f.shard, step)
		}
		f.write(3 + i)
	}
	run.swap(t, f.ctx, f.shard, swapValue, swapNull, swapLength)
	f.write(8)

	f.setIndex(true)
	f.write(9)
	f.requireMatch("after the flip")

	f.shard = reloadShardFromDisk(t, f.ctx, f.idx, f.shard, f.class)
	f.write(10)
	f.requireMatch("after a restart")
}

func TestEnableRunBuildsTheLengthAndNullIndexes(t *testing.T) {
	flags := []struct {
		name         string
		null, length bool
	}{
		{"null-state only", true, false},
		{"property-length only", false, true},
		{"both", true, true},
	}
	for _, enable := range nullLengthEnables() {
		for _, f := range flags {
			t.Run(enable.name+"/"+f.name, func(t *testing.T) {
				runNullLengthScenario(t, nullLengthScenario{
					enable: enable, null: f.null, length: f.length,
					steps: []string{swapValue, swapNull, swapLength},
				})
			})
		}
	}
	t.Run("a payload property that already has an index gets no companion", func(t *testing.T) {
		runNullLengthScenario(t, nullLengthScenario{
			enable: nullLengthEnables()[1], null: true, length: true, withIndexedQ: true,
			steps: []string{swapValue, swapNull, swapLength},
		})
	})
}

// Writes land after every single swap, in every order, and across restarts in
// each mixed state, so no step reaches a bucket that is not installed and no
// mirrored write is lost.
func TestEnableRunKeepsLengthAndNullCorrectUnderWrites(t *testing.T) {
	for _, steps := range [][]string{
		{swapNull, swapLength, swapValue},
		{swapLength, swapNull, swapValue},
		{swapNull, swapValue, swapLength},
		{swapValue, restartStep, swapNull, swapLength},
		{swapNull, swapLength, restartStep, swapValue},
		{swapNull, restartStep, swapLength, swapValue},
		{swapValue, swapNull, swapLength, restartStep},
		{swapValue, swapNull, swapLength, restartFinishedStep},
	} {
		t.Run(fmt.Sprint(steps), func(t *testing.T) {
			runNullLengthScenario(t, nullLengthScenario{
				enable: nullLengthEnables()[0], null: true, length: true, steps: steps,
			})
		})
	}
}

func lsmDirsContaining(t *testing.T, s *Shard, part string) []string {
	t.Helper()
	entries, err := os.ReadDir(s.pathLSM())
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if strings.Contains(e.Name(), part) {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestACancelledEnableRunLeavesNoLengthOrNullState(t *testing.T) {
	f := newNullLengthFixture(t, nullLengthScenario{enable: nullLengthEnables()[0], null: true, length: true})
	run := f.run(1)
	run.each(t, func(task *ShardReindexTaskGeneric) error { return task.RunReindexOnlyOnShard(f.ctx, f.shard) })
	run.each(t, func(task *ShardReindexTaskGeneric) error { return task.RunPrepareOnShard(f.ctx, f.shard) })
	require.NotEmpty(t, lsmDirsContaining(t, f.shard, "__enable_nullstate_"), "the run has to have built a companion")

	// The cancel drops the run's records before it sweeps.
	for _, task := range run.tasks {
		require.NoError(t, f.shard.migrationRecordStore().Remove(task.migrationRecordKey()))
	}
	require.NoError(t, f.shard.CleanStalePartialReindexState(f.ctx, "p", "filterable", nil))
	require.Empty(t, lsmDirsContaining(t, f.shard, "property_p_nullState"))
	require.Empty(t, lsmDirsContaining(t, f.shard, "property_p_propertyLength"))

	f.run(2).all(t, f.ctx, f.shard)
	f.write(1)
	f.setIndex(true)
	f.requireMatch("after a resubmit")
}

// An older binary cannot read these records, so the release note tells
// operators how many loads on this version close them.
func TestLengthAndNullRecordsCloseOnTheSecondLoadAfterTheFlip(t *testing.T) {
	f := newNullLengthFixture(t, nullLengthScenario{enable: nullLengthEnables()[0], null: true, length: true})
	f.run(1).all(t, f.ctx, f.shard)
	f.setIndex(true)
	companionRecords := func() []string {
		entries, err := os.ReadDir(filepath.Join(f.shard.pathLSM(), migrationsDir, "records"))
		require.NoError(t, err)
		var out []string
		for _, e := range entries {
			// Literal: the file names are what an older binary reads.
			if strings.Contains(e.Name(), "_null_state_") || strings.Contains(e.Name(), "_property_length_") {
				out = append(out, e.Name())
			}
		}
		return out
	}
	for load, want := range []int{2, 0} {
		f.shard = restartForRun(t, f.ctx, f.idx, f.shard, f.class, distributedtask.TaskStatusFinished)
		f.shard.migrations().ReconcileWithClusterTasks(f.ctx, []*distributedtask.Task{{
			Namespace:      ReindexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "enable-" + f.class.Class, Version: 1},
			Status:         distributedtask.TaskStatusFinished,
		}})
		require.Len(t, companionRecords(), want, "after load %d", load+1)
	}
	f.write(1)
	f.requireMatch("once the records are closed")
}

// An unloaded shard is swept only when this says so.
func TestALeftoverLengthOrNullSidecarIsStale(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	for _, dir := range []string{
		"property_p_nullState__enable_nullstate_ingest_1",
		"property_p_propertyLength__enable_proplength_reindex_1",
	} {
		lsm := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(lsm, dir), 0o755))
		stale, _ := hasStalePartialReindexState(lsm, "p", "filterable", nil, logger)
		require.True(t, stale, dir)
	}
}

// Dropping every index leaves the buckets behind, untracked; a later enable
// displaces them, with or without a restart in between.
func TestReEnablingAfterDroppingEveryIndexRebuildsLengthAndNull(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			f := newNullLengthFixture(t, nullLengthScenario{enable: nullLengthEnables()[0], null: true, length: true})
			f.run(1).all(t, f.ctx, f.shard)
			f.setIndex(true)
			f.idx.db.indices[indexID(f.idx.Config.ClassName)] = f.idx
			installTestMigrationTaskSources(f.ctx, f.idx.db, nil, &distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "enable-" + f.class.Class, Version: 1},
				Status:         distributedtask.TaskStatusFinished,
			})

			f.setIndex(false)
			o := f.shard.SnapshotPropertyOverlay([]string{"p"})["p"]
			require.False(t, o.TracksNullState || o.TracksPropertyLength, "nothing keeps the buckets current any more")
			f.write(1)
			if restart {
				f.shard = reloadShardFromDisk(t, f.ctx, f.idx, f.shard, f.class)
			}

			f.run(2).all(t, f.ctx, f.shard)
			f.write(2)
			f.setIndex(true)
			f.requireMatch("after the re-enable")
		})
	}
}

func TestEnableRunsBuildALengthAndNullTaskOnlyWhereMissing(t *testing.T) {
	f, tr := false, true
	prop := func(dt schema.DataType, filterable, searchable, rangeable *bool) *models.Property {
		return &models.Property{
			Name: "p", DataType: dt.PropString(),
			IndexFilterable: filterable, IndexSearchable: searchable, IndexRangeFilters: rangeable,
		}
	}
	none := func(dt schema.DataType) *models.Property { return prop(dt, &f, &f, &f) }
	for _, tc := range []struct {
		name               string
		prop               *models.Property
		null, length       bool
		wantNull, wantLeng bool
	}{
		{name: "text without index", prop: none(schema.DataTypeText), null: true, length: true, wantNull: true, wantLeng: true},
		{name: "text, collection tracks null only", prop: none(schema.DataTypeText), null: true, wantNull: true},
		{name: "text, collection tracks length only", prop: none(schema.DataTypeText), length: true, wantLeng: true},
		{name: "text, collection tracks neither", prop: none(schema.DataTypeText)},
		{name: "text array", prop: none(schema.DataTypeTextArray), null: true, length: true, wantNull: true, wantLeng: true},
		{name: "int has no length", prop: none(schema.DataTypeInt), null: true, length: true, wantNull: true},
		{name: "date has no length", prop: none(schema.DataTypeDate), null: true, length: true, wantNull: true},
		{name: "bool has no length", prop: none(schema.DataTypeBoolean), null: true, length: true, wantNull: true},
		{name: "geo has neither", prop: none(schema.DataTypeGeoCoordinates), null: true, length: true},
		{name: "nested object has neither", prop: none(schema.DataTypeObject), null: true, length: true},
		{name: "filterable already", prop: prop(schema.DataTypeText, &tr, &f, &f), null: true, length: true},
		{name: "filterable by default", prop: prop(schema.DataTypeText, nil, &f, &f), null: true, length: true},
		{name: "searchable already", prop: prop(schema.DataTypeText, &f, &tr, &f), null: true, length: true},
		{name: "rangeable already", prop: prop(schema.DataTypeInt, &f, &f, &tr), null: true, length: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			class := &models.Class{
				Class:               "C",
				InvertedIndexConfig: &models.InvertedIndexConfig{IndexNullState: tc.null, IndexPropertyLength: tc.length},
				Properties:          []*models.Property{tc.prop},
			}
			null, length := nullLengthCompanionsIn(class, "p")
			require.Equal(t, tc.wantNull, null, "null-state")
			require.Equal(t, tc.wantLeng, length, "property length")
		})
	}

	logger, _ := logrustest.NewNullLogger()
	companions := func(_, prop string) (bool, bool) { return prop != "indexed", prop == "text" }
	codes := func(mt ReindexMigrationType, hook func(string, string) (bool, bool)) map[MigrationStrategyCode][]string {
		tasks, err := buildMigrationTasks(logger, nil, 3, &ReindexTaskPayload{
			MigrationType: mt, Collection: "C", Properties: []string{"text", "num", "indexed"},
			TargetTokenization: models.PropertyTokenizationWord, BucketStrategy: "inverted",
		}, func(string, string) bool { return true }, hook)
		if err != nil {
			return nil
		}
		out := map[MigrationStrategyCode][]string{}
		for _, task := range tasks {
			out[task.strategy.StrategyCode()] = slices.Sorted(maps.Keys(task.config.selectedPropsByCollection["C"]))
		}
		return out
	}
	for _, mt := range []ReindexMigrationType{ReindexTypeEnableFilterable, ReindexTypeEnableSearchable, ReindexTypeEnableRangeable} {
		got := codes(mt, companions)
		require.Equal(t, []string{"num", "text"}, got[StrategyCodeNullState], mt)
		require.Equal(t, []string{"text"}, got[StrategyCodePropertyLength], mt)
		require.Len(t, got, 3, mt)
		require.Len(t, codes(mt, allNullLengthCompanions)[StrategyCodeNullState], 3, "recovery builds the superset")
	}
	for _, mt := range []ReindexMigrationType{
		ReindexTypeRepairFilterable, ReindexTypeRepairRangeable, ReindexTypeRebuildSearchable,
		ReindexTypeChangeAlgorithm, ReindexTypeChangeTokenizationFilterable,
	} {
		for code := range codes(mt, allNullLengthCompanions) {
			require.NotContains(t, []MigrationStrategyCode{StrategyCodeNullState, StrategyCodePropertyLength}, code, mt)
		}
	}
}
