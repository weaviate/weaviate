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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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
				gen1 := reindexBucketName(genProp, indexTypeFilterable, 1)
				c.waitFor("t2's build folder to go", func() bool {
					return !c.folderExists(c.nodes[0], "t2", gen1) && !c.folderExists(c.nodes[1], "t2", gen1) &&
						!c.folderExists(c.nodes[2], "t2", gen1)
				})
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
	require.Equal(t, []string{"", "", ""}, c.servedFolders("t2", genProp, indexTypeSearchable), "no view governs t2")

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
