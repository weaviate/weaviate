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
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	genCompleted = api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_UNIT_COMPLETED
	genProgress  = api.ApplyRequest_TYPE_DISTRIBUTED_TASK_UPDATE_UNIT_PROGRESS
	genPrepAck   = api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_PREPARATION_COMPLETE_ACK
	genMarker    = "crash-marker"
)

func (c *genCluster) buildFolder(root, shard, indexType string, gen uint64) string {
	return filepath.Join(shardPathLSM(filepath.Join(root, indexID(schema.ClassName(c.class))), shard),
		reindexBucketName(genProp, indexType, gen))
}

// unprepend turns the oldest segment of dir back into the .tmp files a prepend
// leaves when it crashes before its rename.
func unprepend(t *testing.T, dir string) {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var segments []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".db" {
			segments = append(segments, e.Name())
		}
	}
	require.NotEmpty(t, segments, dir)
	slices.Sort(segments)
	prefix, _, _ := strings.Cut(segments[0], ".")
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix+".") {
			p := filepath.Join(dir, e.Name())
			require.NoError(t, os.Rename(p, p+".tmp"))
		}
	}
}

func (c *genCluster) markBuild(t *testing.T, root, shard string) {
	require.NoError(t, os.WriteFile(filepath.Join(c.buildFolder(root, shard, indexTypeFilterable, 1), genMarker), nil, 0o644))
}

func (c *genCluster) marked(root, shard string) bool {
	_, err := os.Stat(filepath.Join(c.buildFolder(root, shard, indexTypeFilterable, 1), genMarker))
	return err == nil
}

func (c *genCluster) unmark(t *testing.T, root, shard string) {
	require.NoError(t, os.Remove(filepath.Join(c.buildFolder(root, shard, indexTypeFilterable, 1), genMarker)))
}

func fromNode(node string) func(raw []byte) bool {
	return func(raw []byte) bool {
		var r struct {
			NodeID string `json:"node_id"`
		}
		return json.Unmarshal(raw, &r) == nil && r.NodeID == node
	}
}

func anyEntry([]byte) bool { return true }

func requireNoRefusedCompletion(t *testing.T, n *genNode) {
	require.False(t, n.hasError("already terminal"), "a completion counted as a failure on %s", n.name)
	require.False(t, n.hasError("unit failed"), "a unit failed on %s", n.name)
}

// A node crashes at each point of a run and restarts from its files and its
// own log; the run finishes and every replica answers exactly.
func TestReindexGenerationCrash_Points(t *testing.T) {
	unit2 := MigrationUnitID("t1", "node2")
	for _, point := range []struct {
		name  string
		typ   api.ApplyRequest_Type // the crash waits until node2's entry of typ that matches is held
		match func(raw []byte) bool
		at    func(t *testing.T, c *genCluster, id string)
		// commitHeld lets what node2 proposed before the crash reach the log; otherwise it is lost.
		commitHeld bool
		edit       func(t *testing.T, c *genCluster, root string, types []string)
	}{
		{name: "while its unit builds", typ: genCompleted, match: aboutUnit(unit2)},
		{
			name: "in the middle of the prepend", typ: genCompleted, match: aboutUnit(unit2),
			edit: func(t *testing.T, c *genCluster, root string, types []string) {
				for _, it := range types {
					unprepend(t, c.buildFolder(root, "t1", it, 1))
				}
			},
		},
		{name: "its unit done, before the decision", typ: genPrepAck, match: fromNode("node2")},
		{
			name: "decided, the flip missing from its log", typ: api.ApplyRequest_TYPE_REINDEX_FLIP, match: anyEntry,
			commitHeld: true,
			at:         func(t *testing.T, c *genCluster, id string) { c.waitStatus(id, distributedtask.TaskStatusSwapping) },
		},
		{name: "after the flip", at: func(t *testing.T, c *genCluster, id string) { c.waitGone(id) }},
	} {
		for _, tc := range []struct {
			mt   ReindexMigrationType
			opts genClassOpts
			want genReads
		}{
			{mt: ReindexTypeChangeTokenization, opts: genDefaultOpts(), want: genFieldReads},
			{
				mt: ReindexTypeEnableFilterable, want: genWordReads,
				opts: genClassOpts{searchable: true, tokenization: models.PropertyTokenizationWord},
			},
		} {
			t.Run(string(tc.mt)+" "+point.name, func(t *testing.T) {
				c := newGenCluster(t, tc.opts, 2)
				c.addTenants("t1")
				c.write("t1", 5)
				types, _ := ReindexIndexTypes(tc.mt)
				release := func() {}
				if point.match != nil {
					release = c.hold(point.typ, point.match)
				}
				id, err := c.submit(tc.mt, models.PropertyTokenizationField)
				require.NoError(t, err)
				if point.at != nil {
					point.at(t, c, id)
				}
				if point.match != nil {
					c.awaitHeld(point.typ, point.match, 1)
				}
				c.crash(1)
				if point.commitHeld {
					release()
				}
				var o genRestart
				if point.edit != nil {
					o.edit = func(root string) { point.edit(t, c, root, types) }
				}
				c.restart(1, o)
				release()
				c.waitGone(id)
				c.waitServed("t1", genProp, 1, types...)
				c.requireReads("t1", tc.want, "after the restart")
			})
		}
	}
}

// A unit's completion lands late on a replica that crashed or lost the reply,
// and the replica's build is kept and stays exact.
func TestReindexGenerationCrash_LateCompletion(t *testing.T) {
	unit2 := MigrationUnitID("t1", "node2")
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, c *genCluster)
	}{
		{
			name: "committed on the leader, missing from the crashed node's log",
			run: func(t *testing.T, c *genCluster) {
				release := c.holdUnit(genCompleted, unit2)
				c.awaitHeld(genCompleted, aboutUnit(unit2), 1)
				c.crash(1)
				c.commit(release, genCompleted, unit2, 1)
				c.restart(1, genRestart{})
			},
		},
		{
			name: "applied while the relaunch scans",
			run: func(t *testing.T, c *genCluster) {
				release := c.holdUnit(genCompleted, unit2)
				c.awaitHeld(genCompleted, aboutUnit(unit2), 1)
				c.crash(1)
				c.commit(release, genCompleted, unit2, 1)
				starts := c.unitEntries(genProgress, unit2)
				c.restart(1, genRestart{lag: true})
				c.waitFor("the relaunch to record its start", func() bool { return c.unitEntries(genProgress, unit2) > starts })
				c.catchUp(1)
				c.waitFor("the relaunch's completion", func() bool { return c.unitEntries(genCompleted, unit2) == 2 })
			},
		},
		{
			name: "applied after the relaunch's prepend",
			run: func(t *testing.T, c *genCluster) {
				release := c.holdUnit(genCompleted, unit2)
				c.awaitHeld(genCompleted, aboutUnit(unit2), 1)
				c.crash(1)
				c.commit(release, genCompleted, unit2, 1)
				relaunched := c.holdUnit(genCompleted, unit2)
				c.restart(1, genRestart{lag: true})
				c.awaitHeld(genCompleted, aboutUnit(unit2), 1)
				c.catchUp(1)
				c.commit(relaunched, genCompleted, unit2, 2)
			},
		},
		{
			name: "the start proposed, then a crash before it applied locally: the folder is emptied",
			run: func(t *testing.T, c *genCluster) {
				release := c.holdUnit(genProgress, unit2)
				c.awaitHeld(genProgress, aboutUnit(unit2), 1)
				c.crash(1)
				c.commit(release, genProgress, unit2, 1)
				c.restart(1, genRestart{
					edit: func(root string) { c.markBuild(t, root, "t1") },
					loaded: func(n *genNode) {
						n.awaitStaged("t1")
						require.False(t, c.marked(n.root, "t1"), "a PENDING unit's folder is kept")
					},
				})
			},
		},
		{
			name: "the reply lost in one process, the completion proposed again",
			run: func(t *testing.T, c *genCluster) {
				var lost atomic.Pointer[api.RecordDistributedTaskUnitCompletionRequest]
				c.nodes[1].lose = func(r *api.RecordDistributedTaskUnitCompletionRequest) bool {
					return r.UnitId == unit2 && lost.CompareAndSwap(nil, r)
				}
				release := c.hold(genCompleted, func(raw []byte) bool {
					var r api.RecordDistributedTaskUnitCompletionRequest
					l := lost.Load()
					return json.Unmarshal(raw, &r) == nil && r.UnitId == unit2 && (l == nil || r.FinishedAtUnixMillis != l.FinishedAtUnixMillis)
				})
				c.awaitHeld(genCompleted, aboutUnit(unit2), 1)
				require.NoError(t, c.apply(genCompleted, "", lost.Load()))
				c.commit(release, genCompleted, unit2, 2)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGenCluster(t, genDefaultOpts(), 2)
			c.addTenants("t1")
			c.write("t1", 5)
			// Node1's unit stays open, so a re-sent completion meets its own unit already done, not a finished task.
			releaseNode1 := c.holdUnit(genCompleted, MigrationUnitID("t1", "node1"))
			id, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
			require.NoError(t, err)
			tc.run(t, c)
			releaseNode1()
			c.waitGone(id)
			c.waitServed("t1", genProp, 1, indexTypeFilterable, indexTypeSearchable)
			c.requireReads("t1", genFieldReads, "after the late completion")
			requireNoRefusedCompletion(t, c.nodes[1])
		})
	}
}

// A unit of a run that took over a dead run's generation starts only once its
// folder was staged, so the dead run's folder never survives into its build.
func TestReindexGenerationCrash_StartAfterStaging(t *testing.T) {
	c := newGenCluster(t, genDefaultOpts(), 1)
	c.addTenants("t1", "t2")
	c.write("t1", 3)
	c.write("t2", 3)
	unit := MigrationUnitID("t2", "node1")
	releaseT2 := c.holdUnit(genCompleted, unit)
	id, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
	require.NoError(t, err)
	c.waitSwitched(id, "t1")
	c.awaitHeld(genCompleted, aboutUnit(unit), 1)
	held := c.nodes[0].shard("t2").acquireView() // holds off the retire of the dead run's folder
	require.NoError(t, c.cancel(id))
	releaseT2()
	next, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
	require.NoError(t, err)
	require.Equal(t, uint64(1), c.generation(next))
	require.Never(t, func() bool { return c.task(next).Units[unit].Status != distributedtask.UnitStatusPending },
		3*time.Second, 10*time.Millisecond, "the unit started before its folder was staged")

	c.crash(0)
	c.restart(0, genRestart{
		edit: func(root string) { c.markBuild(t, root, "t2") },
		loaded: func(n *genNode) {
			n.awaitStaged("t2")
			require.False(t, c.marked(n.root, "t2"), "the dead run's folder is kept")
		},
	})
	held.release()
	c.waitGone(next)
	c.waitServed("t2", genProp, 1, indexTypeFilterable, indexTypeSearchable)
	c.requireReads("t2", genReads{0, 3, 0, 3}, "t2 after the takeover")
}

// After a restart each shard of a single-tenant run keeps its folder only if
// its own unit started.
func TestReindexGenerationCrash_UnitStartedPerShard(t *testing.T) {
	o := genDefaultOpts()
	o.singleTenantShards = 2
	c := newGenCluster(t, o, 1)
	started, pending := c.shards[0], c.shards[1]
	for _, s := range c.shards {
		c.write(s, 3)
	}
	releaseDone := c.holdUnit(genCompleted, MigrationUnitID(started, "node1"))
	releaseStart := c.holdUnit(genProgress, MigrationUnitID(pending, "node1"))
	id, err := c.submit(ReindexTypeChangeTokenization, models.PropertyTokenizationField)
	require.NoError(t, err)
	c.awaitHeld(genCompleted, aboutUnit(MigrationUnitID(started, "node1")), 1)
	c.awaitHeld(genProgress, aboutUnit(MigrationUnitID(pending, "node1")), 1)

	c.crash(0)
	c.restart(0, genRestart{
		edit: func(root string) {
			c.markBuild(t, root, started)
			c.markBuild(t, root, pending)
		},
		loaded: func(n *genNode) {
			n.awaitStaged(started)
			n.awaitStaged(pending)
			require.True(t, c.marked(n.root, started), "the started unit's folder is emptied")
			require.False(t, c.marked(n.root, pending), "the PENDING unit's folder is kept")
			c.unmark(t, n.root, started)
		},
	})
	releaseDone()
	releaseStart()
	c.waitGone(id)
	for _, s := range c.shards {
		c.waitServed(s, genProp, 1, indexTypeFilterable, indexTypeSearchable)
		c.requireReads(s, genReads{0, 3, 0, 3}, s)
	}
}

// A rangeable build gets its in-memory structure after its last prepend, and
// a relaunch reopens it without, so its next prepend is not refused.
func TestReindexGenerationCrash_RangeableInMemory(t *testing.T) {
	o := genDefaultOpts()
	o.rangeableInMemory = true
	c := newGenCluster(t, o, 1)
	c.addTenants("t1")
	c.write("t1", 5)
	unit := MigrationUnitID("t1", "node1")
	var lost atomic.Bool
	c.nodes[0].lose = func(r *api.RecordDistributedTaskUnitCompletionRequest) bool {
		return r.UnitId == unit && lost.CompareAndSwap(false, true)
	}
	release := c.holdUnit(genCompleted, unit)
	id, err := c.submit(ReindexTypeEnableRangeable, "")
	require.NoError(t, err)
	c.awaitHeld(genCompleted, aboutUnit(unit), 1) // the relaunch's, in the same process
	staged := func(n *genNode) *stagedBuild {
		builds := n.shard("t1").loadView().staged
		require.Len(t, builds, 1)
		return builds[0]
	}
	require.True(t, staged(c.nodes[0]).bucket.RangeableServesFromMemory(), "no in-memory structure after the build")

	c.crash(0)
	c.restart(0, genRestart{loaded: func(n *genNode) {
		n.awaitStaged("t1")
		require.False(t, staged(n).bucket.RangeableServesFromMemory(), "a started build reopened with its structure")
	}})
	release()
	c.waitGone(id)
	c.waitServed("t1", genNum, 1, indexTypeRangeable)
	c.requireReads("t1", genWordReads, "after the relaunch")
	b := c.nodes[0].shard("t1").loadView().served[reindexBucketName(genNum, indexTypeRangeable, 0)]
	require.True(t, b.RangeableServesFromMemory())
}
