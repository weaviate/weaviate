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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

var (
	genWordReads  = genReads{filterHello: 5, filterValue: 5, bm25Hello: 5, numBelow3: 3}
	genFieldReads = genReads{filterHello: 0, filterValue: 5, bm25Hello: 0, numBelow3: 3}
)

// Each type builds generation 1 on every replica, switches every tenant to it
// and flips the class, and a reload serves the same.
func TestReindexGenerationRuns_EveryType(t *testing.T) {
	field := models.PropertyTokenizationField
	withOpts := func(edit func(*genClassOpts)) genClassOpts {
		o := genDefaultOpts()
		edit(&o)
		return o
	}
	for _, tc := range []struct {
		mt    ReindexMigrationType
		tok   string
		opts  genClassOpts
		want  genReads
		check func(t *testing.T, p *models.Property, c *models.Class)
	}{
		{
			mt: ReindexTypeChangeTokenization, tok: field, opts: genDefaultOpts(), want: genFieldReads,
			check: func(t *testing.T, p *models.Property, _ *models.Class) { require.Equal(t, field, p.Tokenization) },
		},
		{
			mt: ReindexTypeChangeTokenizationFilterable, tok: field, opts: genDefaultOpts(),
			want:  genReads{filterHello: 0, filterValue: 5, bm25Hello: 5, numBelow3: 3},
			check: func(t *testing.T, p *models.Property, _ *models.Class) { require.Equal(t, field, p.Tokenization) },
		},
		{
			mt: ReindexTypeEnableFilterable, opts: withOpts(func(o *genClassOpts) { o.filterable = false }), want: genWordReads,
			check: func(t *testing.T, p *models.Property, _ *models.Class) { require.True(t, *p.IndexFilterable) },
		},
		{
			mt: ReindexTypeEnableSearchable, tok: models.PropertyTokenizationWord,
			opts: withOpts(func(o *genClassOpts) { o.searchable = false }), want: genWordReads,
			check: func(t *testing.T, p *models.Property, _ *models.Class) { require.True(t, *p.IndexSearchable) },
		},
		{
			mt: ReindexTypeEnableRangeable, opts: genDefaultOpts(), want: genWordReads,
			check: func(t *testing.T, p *models.Property, _ *models.Class) { require.True(t, *p.IndexRangeFilters) },
		},
		{mt: ReindexTypeRepairRangeable, opts: withOpts(func(o *genClassOpts) { o.rangeable = true }), want: genWordReads},
		{
			mt: ReindexTypeChangeAlgorithm, opts: withOpts(func(o *genClassOpts) { o.mapSearchable = true }), want: genWordReads,
			check: func(t *testing.T, p *models.Property, c *models.Class) {
				require.True(t, *p.SearchableBlockmax)
				require.True(t, c.InvertedIndexConfig.UsingBlockMaxWAND)
			},
		},
		{mt: ReindexTypeRebuildSearchable, opts: genDefaultOpts(), want: genWordReads},
		{mt: ReindexTypeRepairFilterable, opts: genDefaultOpts(), want: genWordReads},
	} {
		t.Run(string(tc.mt), func(t *testing.T) {
			c := newGenCluster(t, tc.opts, 3)
			c.addTenants("t1", "t2")
			c.write("t1", 5)
			c.write("t2", 5)
			id, err := c.submit(tc.mt, tc.tok)
			require.NoError(t, err)
			c.waitGone(id)

			prop := genPropFor(tc.mt)
			types, _ := ReindexIndexTypes(tc.mt)
			live := map[string]uint64{}
			for _, it := range types {
				live[it] = 1
			}
			require.Equal(t, map[string]map[string]uint64{prop: live}, c.nodes[0].sr.ClassLive(c.class))
			if tc.check != nil {
				tc.check(t, c.classProp(prop), c.nodes[0].sr.ReadOnlyClass(c.class))
			}
			for _, tenant := range []string{"t1", "t2"} {
				c.waitServed(tenant, prop, 1, types...)
				c.requireReads(tenant, tc.want, "after the switch")
				c.reloadAll(tenant)
				c.requireReads(tenant, tc.want, "after reload")
				for _, it := range types {
					gen1 := reindexBucketName(prop, it, 1)
					require.Equal(t, []string{gen1, gen1, gen1}, c.servedFolders(tenant, prop, it), "%s %s", tenant, it)
				}
			}
		})
	}
}

func hasUnitOn(task *distributedtask.Task, shard string) bool {
	for _, u := range task.Units {
		if u.Shard == shard {
			return true
		}
	}
	return false
}

// A run ends with t1 switched and t2 still building.
func TestReindexGenerationRuns_PartialRun(t *testing.T) {
	field := models.PropertyTokenizationField
	word3, field3 := genReads{3, 3, 3, 3}, genReads{0, 3, 0, 3}
	cancel := func(c *genCluster, id string) error { return c.cancel(id) }
	for _, tc := range []struct {
		name string
		mt   ReindexMigrationType
		opts genClassOpts
		end  func(c *genCluster, id string) error
		then func(t *testing.T, c *genCluster, id string)
	}{
		{
			name: "cancelled, then resubmitted, takes over its generation", mt: ReindexTypeChangeTokenization,
			opts: genDefaultOpts(), end: cancel,
			then: func(t *testing.T, c *genCluster, id string) {
				gen := c.generation(id)
				next, err := c.submit(ReindexTypeChangeTokenization, field)
				require.NoError(t, err)
				require.Equal(t, gen, c.generation(next))
				require.Nil(t, c.task(id))
				require.False(t, hasUnitOn(c.task(next), "t1"))
				require.True(t, hasUnitOn(c.task(next), "t2"))
				c.waitGone(next)
				c.waitServed("t2", genProp, gen, indexTypeFilterable, indexTypeSearchable)
				c.requireReads("t1", field3, "t1 after the resubmit")
				c.requireReads("t2", field3, "t2 after the resubmit")
			},
		},
		{
			name: "cancelled, the building tenant serves generation 0 and drops its build", mt: ReindexTypeChangeTokenization,
			opts: genDefaultOpts(), end: cancel,
			then: func(t *testing.T, c *genCluster, id string) {
				c.write("t2", 2)
				for _, it := range []string{indexTypeFilterable, indexTypeSearchable} {
					gen1 := reindexBucketName(genProp, it, 1)
					c.waitFor("t2's build folder "+gen1+" to go", func() bool {
						return !c.folderExists(c.nodes[0], "t2", gen1) && !c.folderExists(c.nodes[1], "t2", gen1) &&
							!c.folderExists(c.nodes[2], "t2", gen1)
					})
				}
				c.requireReads("t2", genReads{5, 5, 5, 5}, "t2 after the cancel")
				c.requireReads("t1", field3, "t1 after the cancel")
			},
		},
		{
			name: "failed enable-filterable, a deleted tenant leaves the switched set", mt: ReindexTypeEnableFilterable,
			opts: genClassOpts{searchable: true, tokenization: models.PropertyTokenizationWord},
			end:  func(c *genCluster, id string) error { return c.failUnitOf(id, "t2") },
			then: func(t *testing.T, c *genCluster, id string) {
				c.waitStatus(id, distributedtask.TaskStatusFailed)
				require.NoError(t, c.deleteTenants("t1"))
				task := c.task(id)
				require.False(t, task != nil && task.GroupSwapDecided("t1"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGenCluster(t, tc.opts, 3)
			c.addTenants("t1", "t2")
			c.write("t1", 3)
			c.write("t2", 3)
			release := c.holdUnitsOf("t2__")
			id, err := c.submit(tc.mt, field)
			require.NoError(t, err)
			c.waitSwitched(id, "t1")
			if tc.mt == ReindexTypeChangeTokenization {
				c.waitServed("t1", genProp, 1, indexTypeFilterable, indexTypeSearchable)
				c.requireReads("t1", field3, "t1 switched")
				c.requireReads("t2", word3, "t2 building")
			}
			require.NoError(t, tc.end(c, id))
			release()
			tc.then(t, c, id)
		})
	}
}

// Turning off the index a cancelled enable run left on t1 makes every tenant
// drop it, and a re-enable builds a new generation.
func TestReindexGenerationRuns_IndexOff(t *testing.T) {
	c := newGenCluster(t, genClassOpts{searchable: true, tokenization: models.PropertyTokenizationWord}, 1)
	n := c.nodes[0]
	n.sm.SetMutationGuard(n.dtm)
	c.addTenants("t1", "t2")
	c.write("t1", 3)
	c.write("t2", 3)
	release := c.holdUnitsOf("t2__")
	id, err := c.submit(ReindexTypeEnableFilterable, "")
	require.NoError(t, err)
	c.waitSwitched(id, "t1")
	require.NoError(t, c.cancel(id))
	release()
	c.waitStatus(id, distributedtask.TaskStatusCancelled)
	gen1 := reindexBucketName(genProp, indexTypeFilterable, 1)
	require.True(t, c.folderExists(n, "t1", gen1))

	off := false
	require.NoError(t, c.apply(api.ApplyRequest_TYPE_UPDATE_PROPERTY, c.class, api.UpdatePropertyRequest{
		Property: &models.Property{Name: genProp, IndexFilterable: &off}, FieldsToUpdate: []string{api.PropertyFieldIndexFilterable},
	}))
	require.Equal(t, map[string]map[string]uint64{genProp: {indexTypeFilterable: 1}}, n.sr.ClassLive(c.class))
	require.Nil(t, c.task(id))
	c.waitFor("t1's generation-1 folder to go", func() bool { return !c.folderExists(n, "t1", gen1) })

	n.reload("t1")
	c.write("t1", 1)
	next, err := c.submit(ReindexTypeEnableFilterable, "")
	require.NoError(t, err)
	require.Equal(t, uint64(2), c.generation(next))
	c.waitGone(next)
	hello := n.filter(genProp, "hello", schema.DataTypeText, filters.OperatorEqual)
	require.Equal(t, 4, n.count("t1", hello))
	require.Equal(t, 3, n.count("t2", hello))
}

// The reload after a snapshot install refreshes loaded shards, since the
// install changes cluster state without the applies that send notices.
func TestReindexGenerationRuns_ReloadAfterSnapshotInstall(t *testing.T) {
	c := newGenCluster(t, genDefaultOpts(), 1)
	n := c.nodes[0]
	c.addTenants("t1")
	plain, gen1 := reindexBucketName(genProp, indexTypeFilterable, 0), reindexBucketName(genProp, indexTypeFilterable, 1)
	serves := func(n *genNode) bool { return servedFolder(n.shard("t1").loadView(), plain) == gen1 }
	// The test handles the queue itself: the loop could otherwise take t1's load
	// notice after the flip and serve generation 1 without the reload.
	r := n.db.reindexRefresh
	r.shutdown()
	handleQueued := func() (classQueued bool) {
		for coll, shards := range r.takeWork(time.Now().Add(time.Hour)) {
			r.refreshCollection(c.ctx, coll, shards)
			classQueued = classQueued || coll == c.class
		}
		return classQueued
	}
	handleQueued()
	require.NoError(t, c.apply(api.ApplyRequest_TYPE_REINDEX_FLIP, c.class, api.ReindexFlipRequest{
		Prop: genProp, Gen: 1, IndexTypes: []string{indexTypeFilterable},
	}))
	require.False(t, handleQueued(), "a flip sends no notice")
	require.False(t, serves(n))

	n.sm.ReloadDBFromSchema()
	require.True(t, handleQueued(), "the reload sends a notice")
	require.True(t, serves(n))

	// A load opens only generation 1, so the next reload's property probe must ask the view.
	c.crash(0)
	n = c.restart(0, genRestart{edit: func(root string) {
		require.NoError(t, os.RemoveAll(filepath.Join(shardPathLSM(filepath.Join(root, indexID(schema.ClassName(c.class))), "t1"), plain)))
	}})
	require.True(t, serves(n))
	n.sm.ReloadDBFromSchema()
	require.False(t, c.folderExists(n, "t1", plain), "the reload added the property again")
}

// A resubmit that takes over a run whose tenants all switched has nothing to build, and still finishes.
func TestReindexGenerationRuns_EmptyTakeover(t *testing.T) {
	field := models.PropertyTokenizationField
	singleTenant := genDefaultOpts()
	singleTenant.singleTenantShards = 1
	for _, tc := range []struct {
		name            string
		opts            genClassOpts
		mt              ReindexMigrationType
		tok             string
		first, resubmit []string // tenants of each run
		end             func(t *testing.T, c *genCluster, id string)
		want            distributedtask.TaskStatus
		then            func(t *testing.T, c *genCluster, next string)
	}{
		{
			name: "full run failed before its flip, the takeover flips", opts: singleTenant,
			mt: ReindexTypeChangeTokenization, tok: field, want: distributedtask.TaskStatusSwapping,
			end: func(t *testing.T, c *genCluster, id string) {
				c.waitStatus(id, distributedtask.TaskStatusSwapping)
				require.NoError(t, c.fail(id))
				c.waitStatus(id, distributedtask.TaskStatusFailed)
			},
			then: func(t *testing.T, c *genCluster, next string) {
				c.waitGone(next)
				require.Equal(t, map[string]map[string]uint64{genProp: {indexTypeFilterable: 1, indexTypeSearchable: 1}},
					c.nodes[0].sr.ClassLive(c.class))
				require.Equal(t, field, c.classProp(genProp).Tokenization)
			},
		},
		{
			name: "scoped run cancelled with its tenant switched, the takeover is done", opts: genDefaultOpts(),
			mt: ReindexTypeRebuildSearchable, first: []string{"t1", "t2"}, resubmit: []string{"t1"},
			want: distributedtask.TaskStatusFinished,
			end: func(t *testing.T, c *genCluster, id string) {
				c.waitSwitched(id, "t1")
				require.NoError(t, c.cancel(id))
			},
			then: func(t *testing.T, c *genCluster, next string) {
				task := c.task(next)
				require.Nil(t, task.Units)
				require.True(t, task.GroupSwapDecided("t1"))
				c.waitServed("t1", genProp, 1, indexTypeSearchable)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGenCluster(t, tc.opts, 1)
			shard := "t1"
			if len(c.shards) > 0 {
				shard = c.shards[0]
			} else {
				c.addTenants("t1", "t2")
				c.write("t2", 3)
			}
			c.write(shard, 3)
			holdFlip := c.hold(api.ApplyRequest_TYPE_REINDEX_FLIP, nil)
			holdT2 := c.holdUnitsOf("t2__")
			id, err := c.submit(tc.mt, tc.tok, tc.first...)
			require.NoError(t, err)
			tc.end(t, c, id)
			holdT2()
			next, err := c.submit(tc.mt, tc.tok, tc.resubmit...)
			require.NoError(t, err)
			require.Equal(t, uint64(1), c.generation(next))
			require.Equal(t, tc.want, c.task(next).Status)
			holdFlip()
			tc.then(t, c, next)
		})
	}
}

// A tenant created during a full run starts switched; one created just before the submit gets units.
func TestReindexGenerationRuns_NewTenant(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  func(c *genCluster, id string) error // nil lets the run finish
	}{
		{name: "created in the entry before the submit"},
		{name: "born mid-run, then cancelled", end: func(c *genCluster, id string) error { return c.cancel(id) }},
		{name: "born mid-run, then failed", end: func(c *genCluster, id string) error { return c.failUnitOf(id, "t1") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGenCluster(t, genDefaultOpts(), 3)
			c.addTenants("t1")
			c.write("t1", 3)
			if tc.end == nil {
				c.addTenants("t3")
				c.write("t3", 2)
			}
			release := c.holdUnitsOf("")
			id, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
			require.NoError(t, err)
			if tc.end == nil {
				require.True(t, hasUnitOn(c.task(id), "t3"))
				release()
				c.waitGone(id)
			} else {
				c.addTenants("t3")
				c.write("t3", 2)
				require.NoError(t, tc.end(c, id))
				release()
				c.waitFor("the run to end", func() bool { return c.task(id).Status.IsTerminal() })
				require.Equal(t, models.PropertyTokenizationWord, c.classProp(genProp).Tokenization)
			}
			c.reloadAll("t3")
			c.requireReads("t3", genReads{0, 2, 0, 2}, "t3")
		})
	}
}

// A tenant-scoped rebuild never flips and is kept compact until a full run covers it.
func TestReindexGenerationRuns_ScopedRebuild(t *testing.T) {
	c := newGenCluster(t, genDefaultOpts(), 3)
	c.addTenants("t1", "t2")
	c.write("t1", 3)
	c.write("t2", 3)
	scoped, err := c.submit(ReindexTypeRebuildSearchable, "", "t1")
	require.NoError(t, err)
	c.waitStatus(scoped, distributedtask.TaskStatusFinished)
	kept := c.task(scoped)
	require.Nil(t, kept.Units)
	require.NotNil(t, kept.Summary)
	require.True(t, kept.GroupSwapDecided("t1"))
	require.Empty(t, c.nodes[0].sr.ClassLive(c.class))
	c.waitServed("t1", genProp, 1, indexTypeSearchable)
	plain := reindexBucketName(genProp, indexTypeSearchable, 0)
	require.Equal(t, []string{plain, plain, plain}, c.servedFolders("t2", genProp, indexTypeSearchable), "t2 keeps its plain folder")

	full, err := c.submit(ReindexTypeRebuildSearchable, "")
	require.NoError(t, err)
	require.Equal(t, uint64(2), c.generation(full))
	c.waitGone(full)
	require.Nil(t, c.task(scoped))
	for _, tenant := range []string{"t1", "t2"} {
		c.waitServed(tenant, genProp, 2, indexTypeSearchable)
		c.requireReads(tenant, genReads{3, 3, 3, 3}, tenant)
	}
}

// One node's build of a repair-filterable run holds every object, whatever writes race it.
func TestReindexGenerationRuns_Build(t *testing.T) {
	ctx := context.Background()
	hello := func(n *genNode) int {
		return n.count("t1", n.filter(genProp, "hello", schema.DataTypeText, filters.OperatorEqual))
	}
	for _, tc := range []struct {
		name     string
		before   func(c *genCluster) []*storobj.Object
		during   func(t *testing.T, c *genCluster, id string, objs []*storobj.Object)
		holdView bool // a write holding a view from before staging
		want     map[string]int
	}{
		{
			name:   "deletes and updates during the build",
			before: func(c *genCluster) []*storobj.Object { return c.write("t1", 2000) },
			during: func(t *testing.T, c *genCluster, _ string, objs []*storobj.Object) {
				s := c.nodes[0].shard("t1")
				for i, o := range objs[:1000] {
					if i%2 == 0 {
						require.NoError(t, s.DeleteObject(ctx, o.ID(), time.Now()))
						continue
					}
					upd := genObject(c.class, "t1", "other words", 0)
					upd.Object.ID = o.ID()
					require.NoError(t, s.PutObject(ctx, upd))
				}
			},
			want: map[string]int{"hello": 1000, "other": 500},
		},
		{
			name: "an object stamped ahead of this node's clock",
			before: func(c *genCluster) []*storobj.Object {
				obj := genObject(c.class, "t1", genValue, 0)
				obj.Object.LastUpdateTimeUnix = time.Now().Add(time.Hour).UnixMilli()
				c.put("t1", obj)
				return c.write("t1", 2)
			},
			want: map[string]int{"hello": 3},
		},
		{
			name:     "a write that loaded a view before staging holds off the scan",
			before:   func(c *genCluster) []*storobj.Object { return c.write("t1", 2) },
			holdView: true,
			during: func(t *testing.T, c *genCluster, id string, _ []*storobj.Object) {
				c.waitFor("the unit to start", func() bool {
					u := c.task(id).Units[MigrationUnitID("t1", c.nodes[0].name)]
					return u.Status == distributedtask.UnitStatusInProgress
				})
				require.Never(t, func() bool { return c.task(id).Status != distributedtask.TaskStatusStarted },
					300*time.Millisecond, 10*time.Millisecond)
			},
			want: map[string]int{"hello": 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGenCluster(t, genDefaultOpts(), 1)
			c.addTenants("t1")
			objs := tc.before(c)
			var held *shardView
			if tc.holdView {
				held = c.nodes[0].shard("t1").acquireView()
			}
			id, err := c.submit(ReindexTypeRepairFilterable, "")
			require.NoError(t, err)
			if tc.during != nil {
				tc.during(t, c, id, objs)
			}
			if held != nil {
				held.release()
			}
			c.waitGone(id)
			n := c.nodes[0]
			c.waitServed("t1", genProp, 1, indexTypeFilterable)
			require.Equal(t, tc.want["hello"], hello(n))
			require.Equal(t, tc.want["other"], n.count("t1", n.filter(genProp, "other", schema.DataTypeText, filters.OperatorEqual)))
		})
	}
}

// A cancel that lands while a unit builds into its scratch leaves none behind, and a resubmit builds the tenant again.
func TestReindexGenerationRuns_CancelDuringScan(t *testing.T) {
	c := newGenCluster(t, genDefaultOpts(), 1)
	c.addTenants("t1", "t2")
	c.write("t1", 3)
	c.write("t2", 5000)
	start := c.holdUnit(genProgress, MigrationUnitID("t2", "node1"))
	id, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
	require.NoError(t, err)
	c.waitSwitched(id, "t1")
	s := c.nodes[0].shard("t2")
	scratch := func() bool {
		for _, it := range []string{indexTypeFilterable, indexTypeSearchable} {
			name := reindexBucketName(genProp, it, 1) + reindexBuildSuffix
			if _, err := os.Stat(filepath.Join(s.pathLSM(), name)); s.store.Bucket(name) != nil || err == nil {
				return true
			}
		}
		return false
	}
	start()
	c.waitFor("the scan to start", scratch)
	require.NoError(t, c.cancel(id))
	c.waitFor("the scratch to go", func() bool { return !scratch() })

	next, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
	require.NoError(t, err)
	require.Equal(t, uint64(1), c.generation(next))
	c.waitGone(next)
	c.waitServed("t2", genProp, 1, indexTypeFilterable, indexTypeSearchable)
	c.requireReads("t2", genReads{0, 5000, 0, 3}, "t2 after the resubmit")
}

// A second scan layer, placed under live updates and deletes as a relaunch
// places it, changes no answer of any index type.
func TestReindexGenerationRuns_DuplicateScanLayer(t *testing.T) {
	ctx := context.Background()
	rangeable, mapSearchable := genDefaultOpts(), genDefaultOpts()
	rangeable.rangeable, mapSearchable.mapSearchable = true, true
	word := genReads{filterHello: 80, filterValue: 80, bm25Hello: 80, numBelow3: 1}
	for _, tc := range []struct {
		name    string
		mt      ReindexMigrationType
		tok     string
		opts    genClassOpts
		want    genReads
		queries []string // BM25 queries whose scores must match the single-scan build
	}{
		{name: "filterable", mt: ReindexTypeRepairFilterable, opts: genDefaultOpts(), want: word},
		{name: "rangeable", mt: ReindexTypeRepairRangeable, opts: rangeable, want: word},
		{
			name: "map searchable", mt: ReindexTypeChangeTokenization, tok: models.PropertyTokenizationField,
			opts: mapSearchable, want: genReads{filterValue: 20, numBelow3: 1},
			queries: []string{"hello world", "hello hello world", "other words"},
		},
		{
			name: "blockmax searchable", mt: ReindexTypeRebuildSearchable, opts: genDefaultOpts(), want: word,
			queries: []string{"hello", "world", "other"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGenCluster(t, tc.opts, 1)
			n := c.nodes[0]
			c.addTenants("t1", "t2") // t1 gets the second layer, t2 is the single-scan build
			objs := map[string][]*storobj.Object{}
			for _, tenant := range []string{"t1", "t2"} {
				for i := range 100 {
					obj := genObject(c.class, tenant, strings.Repeat("hello ", i%4+1)+"world", int64(i))
					c.put(tenant, obj)
					objs[tenant] = append(objs[tenant], obj)
				}
			}
			release := c.holdUnitsOf("")
			id, err := c.submit(tc.mt, tc.tok)
			require.NoError(t, err)
			c.awaitHeld(genCompleted, anyEntry, 2)
			for tenant, list := range objs {
				for i, obj := range list[:20] {
					if i%2 == 0 {
						require.NoError(t, n.shard(tenant).DeleteObject(ctx, obj.ID(), time.Now()))
						continue
					}
					upd := genObject(c.class, tenant, "other words", int64(i))
					upd.Object.ID = obj.ID()
					require.NoError(t, n.shard(tenant).PutObject(ctx, upd))
				}
			}
			s1 := n.shard("t1")
			for _, b := range s1.loadView().staged {
				require.NoError(t, s1.scanIntoBuild(ctx, b))
			}
			release()
			c.waitGone(id)
			types, _ := ReindexIndexTypes(tc.mt)
			for _, tenant := range []string{"t1", "t2"} {
				c.waitServed(tenant, genPropFor(tc.mt), 1, types...)
				require.Equal(t, tc.want, c.reads(tenant, "")[0], tenant)
			}
			for _, query := range tc.queries {
				dup, single := genScores(t, n, "t1", query), genScores(t, n, "t2", query)
				require.Len(t, dup, len(single), query)
				for num, score := range single {
					require.InEpsilon(t, score, dup[num], 0.01, "%q object %v", query, num)
				}
			}
		})
	}
}

// A scan whose run was cancelled still frees its scratch folder, so a takeover
// of the same generation, which reuses the folder name, can scan again.
func TestReindexGenerationRuns_ScanAfterCancelledScan(t *testing.T) {
	c := newGenCluster(t, genDefaultOpts(), 1)
	c.addTenants("t1")
	c.put("t1", genObject(c.class, "t1", "hello world", 1)) // one object: the scan checks ctx once
	release := c.holdUnitsOf("")
	id, err := c.submit(ReindexTypeRepairFilterable, "")
	require.NoError(t, err)
	c.awaitHeld(genCompleted, anyEntry, 1)
	s := c.nodes[0].shard("t1")
	for _, b := range s.loadView().staged {
		ctx := &cancelAfterCheck{Context: context.Background(), done: make(chan struct{}), armed: func() bool {
			return s.store.Bucket(b.name+reindexBuildSuffix) != nil
		}}
		require.ErrorIs(t, s.scanIntoBuild(ctx, b), context.Canceled)
		require.NoError(t, s.scanIntoBuild(context.Background(), b))
	}
	release()
	c.waitGone(id)
}

// cancelAfterCheck is cancelled right after the first Err check made once armed,
// which lands the cancel between the scan's last check and the scratch close.
type cancelAfterCheck struct {
	context.Context
	done  chan struct{}
	once  sync.Once
	armed func() bool
}

func (c *cancelAfterCheck) Done() <-chan struct{} { return c.done }

func (c *cancelAfterCheck) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
	}
	if c.armed() {
		c.once.Do(func() { close(c.done) })
	}
	return nil
}

// genScores is each BM25 hit's score over the top one: a term's weight counts stale entries
// until compaction, which runs at its own time per tenant, so only relative scores compare.
func genScores(t *testing.T, n *genNode, tenant, query string) map[float64]float32 {
	objs, scores, err := n.shard(tenant).ObjectSearch(context.Background(), 1000, nil,
		&searchparams.KeywordRanking{Type: "bm25", Query: query, Properties: []string{genProp}}, nil, nil,
		additional.Properties{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, objs, query)
	out := map[float64]float32{}
	for i, o := range objs {
		out[o.Properties().(map[string]any)[genNum].(float64)] = scores[i] / scores[0]
	}
	return out
}

// A single-tenant run switches its one group "" on every shard before the flip.
func TestReindexGenerationRuns_SingleTenantCollection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		shards int
		fail   bool
	}{
		{name: "decided, before the flip", shards: 1},
		{name: "failed after the decision, then reloaded", shards: 2, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := genDefaultOpts()
			o.singleTenantShards = tc.shards
			c := newGenCluster(t, o, 3)
			shards := c.shards
			for _, s := range shards {
				c.write(s, 3)
			}
			release := c.hold(api.ApplyRequest_TYPE_REINDEX_FLIP, nil)
			id, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
			require.NoError(t, err)
			c.waitStatus(id, distributedtask.TaskStatusSwapping)
			if tc.fail {
				require.NoError(t, c.fail(id))
				c.waitStatus(id, distributedtask.TaskStatusFailed)
				for _, s := range shards {
					c.reloadAll(s)
				}
				require.Empty(t, c.nodes[0].sr.ClassLive(c.class))
			}
			for _, s := range shards {
				c.waitServed(s, genProp, 1, indexTypeFilterable)
				c.requireReads(s, genReads{0, 3, 0, 3}, s)
			}
			if !tc.fail {
				release()
				c.waitGone(id)
			}
		})
	}
}
