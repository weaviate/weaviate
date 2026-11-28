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
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

func TestReindexTaskRulesAdmit(t *testing.T) {
	const word, field = "word", "field"
	ended := func(id string, version uint64, status distributedtask.TaskStatus, p ReindexTaskPayload, switched ...string) *distributedtask.Task {
		p.ViewServed = true
		raw, err := json.Marshal(p)
		require.NoError(t, err)
		task := &distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: version},
			Status: status, Payload: raw, SwapDecidedGroups: map[string]time.Time{},
		}
		for _, g := range switched {
			task.SwapDecidedGroups[g] = time.UnixMilli(1)
		}
		return task
	}
	run := func(collection, target string, gen uint64, props ...string) ReindexTaskPayload {
		return ReindexTaskPayload{
			Collection: collection, MigrationType: ReindexTypeChangeTokenization, Properties: props,
			TargetTokenization: target, Generation: gen,
		}
	}
	titleTo := func(target string, gen uint64) ReindexTaskPayload { return run("C", target, gen, "title") }
	// A finished full run deletes itself, so only a scoped one stays to be read.
	scopedTo := func(target string, gen uint64) ReindexTaskPayload {
		p := titleTo(target, gen)
		p.Tenants = []string{"t1"}
		return p
	}

	for _, tc := range []struct {
		name     string
		live     map[string]map[string]uint64
		existing []*distributedtask.Task
		legacy   bool // the new run's payload has no ViewServed, as a v1.39.x entry does
		submit   ReindexTaskPayload

		wantErr      error
		wantGen      uint64
		wantSwitched []string
		wantGone     []string
	}{
		{
			name:     "an ended task's generation beats a lower live one",
			live:     map[string]map[string]uint64{"title": {"filterable": 2}},
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFinished, scopedTo(field, 3))},
			submit:   titleTo(word, 0),
			wantGen:  4,
		},
		{
			name:     "a live generation beats a lower task one",
			live:     map[string]map[string]uint64{"title": {"filterable": 1, "searchable": 5}},
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFinished, scopedTo(field, 3))},
			submit:   titleTo(word, 0),
			wantGen:  6,
		},
		{
			name:         "a cancelled same-target run is taken over",
			existing:     []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusCancelled, titleTo(word, 2), "t1")},
			submit:       titleTo(word, 0),
			wantGen:      2,
			wantSwitched: []string{"t1"},
			wantGone:     []string{"old"},
		},
		{
			name:     "a finished scoped same-target run is not taken over",
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFinished, scopedTo(word, 2), "t1")},
			submit:   titleTo(word, 0),
			wantGen:  3,
		},
		{
			name: "of two failed same-target runs the higher generation is taken over",
			existing: []*distributedtask.Task{
				ended("low", 1, distributedtask.TaskStatusFailed, titleTo(word, 2), "t1"),
				ended("high", 2, distributedtask.TaskStatusFailed, titleTo(word, 3), "t2"),
			},
			submit:       titleTo(word, 0),
			wantGen:      3,
			wantSwitched: []string{"t2"},
			wantGone:     []string{"high"},
		},
		{
			name: "a run on a superset of the properties is neither counted nor taken over",
			existing: []*distributedtask.Task{
				ended("wide", 1, distributedtask.TaskStatusFailed, run("C", word, 4, "title", "body"), "t1"),
			},
			submit:  titleTo(word, 0),
			wantGen: 1,
		},
		{
			name:     "another target over a failed run with switched tenants is refused",
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFailed, titleTo(field, 2), "t1")},
			submit:   titleTo(word, 0),
			wantErr:  distributedtask.ErrTaskPartlyMigrated,
		},
		{
			name:     "another target over a finished scoped run with switched tenants is admitted",
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFinished, scopedTo(field, 2), "t1")},
			submit:   titleTo(word, 0),
			wantGen:  3,
		},
		{
			name: "a same-named property in another collection never matches",
			existing: []*distributedtask.Task{
				ended("other", 1, distributedtask.TaskStatusCancelled, run("A", word, 2, "title"), "t1"),
			},
			submit:  run("B", word, 0, "title"),
			wantGen: 1,
		},
		{
			name:     "a payload without ViewServed is admitted unchanged",
			live:     map[string]map[string]uint64{"title": {"filterable": 2}},
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFailed, titleTo(field, 2), "t1")},
			legacy:   true,
			submit:   titleTo(word, 0),
			wantGen:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := json.Marshal(map[string]map[string][]*distributedtask.Task{"tasks": {ReindexNamespace: tc.existing}})
			require.NoError(t, err)
			logger, _ := test.NewNullLogger()
			m := distributedtask.NewManager(distributedtask.ManagerParameters{
				Logger:            logger,
				ConflictDetectors: map[string]distributedtask.ConflictDetector{ReindexNamespace: ReindexDetector{}},
				TaskRules:         map[string]distributedtask.TaskRules{ReindexNamespace: ReindexTaskRules{}},
				ClassLive:         func(string) map[string]map[string]uint64 { return tc.live },
			})
			require.NoError(t, m.Restore(snapshot))
			before := taskIDs(m)

			submit := tc.submit
			submit.ViewServed = !tc.legacy
			payload, err := json.Marshal(submit)
			require.NoError(t, err)
			raw, err := json.Marshal(&api.AddDistributedTaskRequest{
				Namespace: ReindexNamespace, Id: "new", Payload: payload, SubmittedAtUnixMillis: 1,
				UnitSpecs: []*api.UnitSpec{
					{Id: "u1", GroupId: "t1", Shard: "t1", Node: "n1"},
					{Id: "u2", GroupId: "t2", Shard: "t2", Node: "n1"},
				},
			})
			require.NoError(t, err)
			err = m.AddTask(&api.ApplyRequest{SubCommand: raw}, 10)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, before, taskIDs(m))
				return
			}
			require.NoError(t, err)
			tasks := m.LocalDistributedTasks()[ReindexNamespace]
			added := findTask(tasks, "new")
			require.NotNil(t, added)
			h, ok := ReindexHeaderOf(added)
			require.True(t, ok)
			require.Equal(t, tc.wantGen, h.Generation)
			if tc.legacy {
				require.JSONEq(t, string(payload), string(added.Payload))
			}
			var stored ReindexTaskPayload
			require.NoError(t, json.Unmarshal(added.Payload, &stored))
			require.Equal(t, tc.wantGen, stored.Generation, "the payload carries the generation the header does")

			require.Len(t, added.SwapDecidedGroups, len(tc.wantSwitched))
			for _, g := range tc.wantSwitched {
				require.Contains(t, added.SwapDecidedGroups, g)
			}
			for _, u := range added.Units {
				require.NotContains(t, tc.wantSwitched, u.GroupID, "a switched tenant gets no unit")
			}
			require.Len(t, added.Units, 2-len(tc.wantSwitched))

			want := append(before, "new")
			for _, gone := range tc.wantGone {
				want = removeString(want, gone)
			}
			require.ElementsMatch(t, want, taskIDs(m))
		})
	}
}

func reindexRulesManager(t *testing.T, seed ...*distributedtask.Task) *distributedtask.Manager {
	t.Helper()
	snapshot, err := json.Marshal(map[string]map[string][]*distributedtask.Task{"tasks": {ReindexNamespace: seed}})
	require.NoError(t, err)
	logger, _ := test.NewNullLogger()
	m := distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:            logger,
		ConflictDetectors: map[string]distributedtask.ConflictDetector{ReindexNamespace: ReindexDetector{}},
		TaskRules:         map[string]distributedtask.TaskRules{ReindexNamespace: ReindexTaskRules{}},
		ClassLive:         func(string) map[string]map[string]uint64 { return nil },
	})
	require.NoError(t, m.Restore(snapshot))
	return m
}

// reindexSeed builds a task with one unit per switched tenant plus one on t9.
func reindexSeed(t *testing.T, id string, version uint64, status distributedtask.TaskStatus, p ReindexTaskPayload, switched ...string) *distributedtask.Task {
	t.Helper()
	p.ViewServed = p.Generation > 0
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: version},
		Status: status, Payload: raw, NeedsPreparationBarrier: true,
		Units:             map[string]*distributedtask.Unit{"u-t9": {ID: "u-t9", GroupID: "t9", Shard: "t9", NodeID: "n1"}},
		SwapDecidedGroups: map[string]time.Time{},
	}
	for _, g := range switched {
		task.Units["u-"+g] = &distributedtask.Unit{ID: "u-" + g, GroupID: g, Shard: g, NodeID: "n1"}
		task.SwapDecidedGroups[g] = time.UnixMilli(1)
	}
	return task
}

func TestReindexTaskRulesEnded(t *testing.T) {
	const word = "word"
	tok := func(collection string, gen uint64, tenants ...string) ReindexTaskPayload {
		return ReindexTaskPayload{
			Collection: collection, MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
			TargetTokenization: word, Generation: gen, Tenants: tenants,
		}
	}
	rebuild := ReindexTaskPayload{
		Collection: "C", MigrationType: ReindexTypeRebuildSearchable, Properties: []string{"title"}, Generation: 3,
	}
	const (
		failed    = distributedtask.TaskStatusFailed
		cancelled = distributedtask.TaskStatusCancelled
		finished  = distributedtask.TaskStatusFinished
		started   = distributedtask.TaskStatusStarted
		swapping  = distributedtask.TaskStatusSwapping
	)
	cancel := func(t *testing.T, m *distributedtask.Manager) {
		raw, err := json.Marshal(&api.CancelDistributedTaskRequest{
			Namespace: ReindexNamespace, Id: "new", Version: 9, CancelledAtUnixMillis: 1,
		})
		require.NoError(t, err)
		require.NoError(t, m.CancelTask(&api.ApplyRequest{SubCommand: raw}))
	}
	finalize := func(t *testing.T, m *distributedtask.Manager) {
		raw, err := json.Marshal(&api.MarkTaskFinalizedRequest{
			Namespace: ReindexNamespace, Id: "new", Version: 9, FinalizedAtUnixMillis: 1,
		})
		require.NoError(t, err)
		require.NoError(t, m.MarkTaskFinalized(&api.ApplyRequest{SubCommand: raw}))
	}

	for _, tc := range []struct {
		name string
		seed func(t *testing.T) []*distributedtask.Task
		end  func(t *testing.T, m *distributedtask.Manager)

		want        map[string][]string // each remaining task's switched tenants
		wantCompact []string
	}{
		{
			name: "an ended run takes its switched tenants from an older covered run",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexSeed(t, "older", 1, cancelled, tok("C", 2), "t1", "t3"),
					reindexSeed(t, "new", 9, started, tok("C", 3), "t1", "t2"),
				}
			},
			end:         cancel,
			want:        map[string][]string{"older": {"t3"}, "new": {"t1", "t2"}},
			wantCompact: []string{"older", "new"},
		},
		{
			name: "a run that does not cover the older run's index types leaves it alone",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexSeed(t, "older", 1, failed, tok("C", 2), "t1"),
					reindexSeed(t, "new", 9, started, rebuild, "t1"),
				}
			},
			end:         cancel,
			want:        map[string][]string{"older": {"t1"}, "new": {"t1"}},
			wantCompact: []string{"older", "new"},
		},
		{
			name: "overlapping scoped runs then a finished full run leave no task",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexSeed(t, "a", 1, finished, tok("C", 2, "t1"), "t1"),
					reindexSeed(t, "b", 2, failed, tok("C", 3, "t1", "t2"), "t2"),
					reindexSeed(t, "new", 9, swapping, tok("C", 4), "t1", "t2", "t3"),
				}
			},
			end:  finalize,
			want: map[string][]string{},
		},
		{
			name: "a finished full run removes older runs with nothing switched or no generation",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexSeed(t, "empty", 1, failed, tok("C", 2)),
					reindexSeed(t, "legacy", 2, failed, tok("C", 0), "t1"),
					reindexSeed(t, "new", 9, swapping, tok("C", 3), "t1"),
				}
			},
			end:  finalize,
			want: map[string][]string{},
		},
		{
			name: "a finished scoped run is kept and compacted",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{reindexSeed(t, "new", 9, swapping, tok("C", 2, "t1"), "t1")}
			},
			end:         finalize,
			want:        map[string][]string{"new": {"t1"}},
			wantCompact: []string{"new"},
		},
		{
			name: "a same-named property in another collection is left alone",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexSeed(t, "other", 1, failed, tok("A", 2), "t1"),
					reindexSeed(t, "new", 9, started, tok("B", 3), "t1"),
				}
			},
			end:         cancel,
			want:        map[string][]string{"other": {"t1"}, "new": {"t1"}},
			wantCompact: []string{"other", "new"},
		},
		{
			name: "a run without a generation is neither cleaned up after nor compacted",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexSeed(t, "older", 1, failed, tok("C", 2), "t1"),
					reindexSeed(t, "new", 9, started, tok("C", 0), "t1"),
				}
			},
			end:         cancel,
			want:        map[string][]string{"older": {"t1"}, "new": {"t1"}},
			wantCompact: []string{"older"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed := tc.seed(t)
			units := map[string]int{}
			for _, task := range seed {
				units[task.ID] = len(task.Units)
			}
			m := reindexRulesManager(t, seed...)

			tc.end(t, m)

			got := map[string][]string{}
			for _, task := range m.LocalDistributedTasks()[ReindexNamespace] {
				got[task.ID] = slices.Sorted(maps.Keys(task.SwapDecidedGroups))
				compacted := slices.Contains(tc.wantCompact, task.ID)
				require.Equal(t, compacted, task.Units == nil, "task %s compacted", task.ID)
				if compacted {
					require.Equal(t, units[task.ID], sumCounts(task.Summary), "task %s summary", task.ID)
				}
			}
			for id, groups := range tc.want {
				slices.Sort(groups)
				tc.want[id] = groups
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func sumCounts(s *distributedtask.UnitSummary) int {
	total := 0
	for _, n := range s.Counts {
		total += n
	}
	return total
}

// TestReindexCutoverLanded fails a SWAPPING full run through Manager.MarkTaskFailed.
func TestReindexCutoverLanded(t *testing.T) {
	const gen = 2
	searchableAt := func(g uint64) map[string]map[string]uint64 {
		return map[string]map[string]uint64{"title": {"searchable": g}}
	}
	for _, tc := range []struct {
		name       string
		mt         ReindexMigrationType
		generation uint64
		live       map[string]map[string]uint64
		wantFailed bool
	}{
		{name: "Live holds the generation", mt: ReindexTypeRebuildSearchable, generation: gen, live: searchableAt(gen)},
		{name: "Live holds an older generation", mt: ReindexTypeRebuildSearchable, generation: gen, live: searchableAt(1), wantFailed: true},
		{name: "Live is absent", mt: ReindexTypeRebuildSearchable, generation: gen, wantFailed: true},
		{name: "Live is absent for one of two index types", mt: ReindexTypeChangeTokenization, generation: gen, live: searchableAt(gen), wantFailed: true},
		{name: "a run without a generation", mt: ReindexTypeRebuildSearchable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed := reindexSeed(t, "run", 5, distributedtask.TaskStatusSwapping, ReindexTaskPayload{
				Collection: "C", Properties: []string{"title"}, MigrationType: tc.mt, Generation: tc.generation,
			}, "t1")
			snapshot, err := json.Marshal(map[string]map[string][]*distributedtask.Task{"tasks": {ReindexNamespace: {seed}}})
			require.NoError(t, err)
			logger, _ := test.NewNullLogger()
			m := distributedtask.NewManager(distributedtask.ManagerParameters{
				Logger:                  logger,
				ConflictDetectors:       map[string]distributedtask.ConflictDetector{ReindexNamespace: ReindexDetector{}},
				SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{ReindexNamespace: ReindexDetector{}},
				TaskRules:               map[string]distributedtask.TaskRules{ReindexNamespace: ReindexTaskRules{}},
				ClassLive:               func(string) map[string]map[string]uint64 { return tc.live },
			})
			require.NoError(t, m.Restore(snapshot))

			raw, err := json.Marshal(&api.MarkTaskFailedRequest{
				Namespace: ReindexNamespace, Id: "run", Version: 5, Error: "flip failed", FailedAtUnixMillis: 1,
			})
			require.NoError(t, err)
			require.NoError(t, m.MarkTaskFailed(&api.ApplyRequest{SubCommand: raw}))

			task := findTask(m.LocalDistributedTasks()[ReindexNamespace], "run")
			switch {
			case tc.wantFailed:
				require.NotNil(t, task)
				require.Equal(t, distributedtask.TaskStatusFailed, task.Status)
				require.Contains(t, task.SwapDecidedGroups, "t1")
			case tc.generation > 0:
				require.Nil(t, task, "a FINISHED full run removes itself")
			default:
				require.NotNil(t, task)
				require.Equal(t, distributedtask.TaskStatusFinished, task.Status)
			}
		})
	}
}

func TestReindexTaskRulesBornSwitched(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    ReindexTaskPayload
		want bool
	}{
		{name: "a full run", p: ReindexTaskPayload{Generation: 2}, want: true},
		{name: "a scoped run", p: ReindexTaskPayload{Generation: 2, Tenants: []string{"t1"}}},
		{name: "a run without a generation", p: ReindexTaskPayload{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.p
			p.Collection, p.MigrationType, p.Properties = "C", ReindexTypeChangeTokenization, []string{"title"}
			m := reindexRulesManager(t, reindexSeed(t, "run", 1, distributedtask.TaskStatusStarted, p, "t1"))

			m.MarkTenantsBornSwitched("c", []string{"t3"})

			run := findTask(m.LocalDistributedTasks()[ReindexNamespace], "run")
			require.Equal(t, tc.want, run.GroupSwapDecided("t3"))
		})
	}
}

func TestReindexTaskRulesIndexOff(t *testing.T) {
	run := func(collection string, mt ReindexMigrationType, gen uint64) ReindexTaskPayload {
		return ReindexTaskPayload{
			Collection: collection, MigrationType: mt, Properties: []string{"title"}, TargetTokenization: "word", Generation: gen,
		}
	}
	const (
		cancelled  = distributedtask.TaskStatusCancelled
		failed     = distributedtask.TaskStatusFailed
		filterable = indexTypeFilterable
	)
	for _, tc := range []struct {
		name      string
		seed      func(t *testing.T) []*distributedtask.Task
		indexType string
		wantLive  uint64
		wantLeft  []string
	}{
		{
			name: "a partial run of that index type alone is covered and removed",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{reindexSeed(t, "a", 1, cancelled, run("C", ReindexTypeEnableFilterable, 2), "t1")}
			},
			indexType: filterable, wantLive: 2,
		},
		{
			name: "a run of two index types sets Live and keeps its tenants",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{reindexSeed(t, "a", 1, cancelled, run("C", ReindexTypeChangeTokenization, 3), "t1")}
			},
			indexType: filterable, wantLive: 3, wantLeft: []string{"a"},
		},
		{
			name: "the highest generation wins",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexSeed(t, "a", 1, failed, run("C", ReindexTypeEnableFilterable, 2), "t1"),
					reindexSeed(t, "b", 2, cancelled, run("C", ReindexTypeRepairFilterable, 4), "t2"),
				}
			},
			indexType: filterable, wantLive: 4,
		},
		{
			name: "a run with no switched tenant serves nothing",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{reindexSeed(t, "a", 1, cancelled, run("C", ReindexTypeEnableFilterable, 2))}
			},
			indexType: filterable, wantLeft: []string{"a"},
		},
		{
			name: "a run of another index type",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{reindexSeed(t, "a", 1, cancelled, run("C", ReindexTypeEnableFilterable, 2), "t1")}
			},
			indexType: indexTypeSearchable, wantLeft: []string{"a"},
		},
		{
			name: "the same property in another collection",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{reindexSeed(t, "a", 1, cancelled, run("D", ReindexTypeEnableFilterable, 2), "t1")}
			},
			indexType: filterable, wantLeft: []string{"a"},
		},
		{
			name: "a run without a generation",
			seed: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{reindexSeed(t, "a", 1, cancelled, run("C", ReindexTypeEnableFilterable, 0), "t1")}
			},
			indexType: filterable, wantLeft: []string{"a"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := reindexRulesManager(t, tc.seed(t)...)
			require.Equal(t, tc.wantLive, m.TurnIndexOff("C", "title", tc.indexType))
			require.ElementsMatch(t, tc.wantLeft, taskIDs(m))
		})
	}
}

func taskIDs(m *distributedtask.Manager) []string {
	var ids []string
	for _, task := range m.LocalDistributedTasks()[ReindexNamespace] {
		ids = append(ids, task.ID)
	}
	return ids
}

func findTask(tasks []*distributedtask.Task, id string) *distributedtask.Task {
	for _, task := range tasks {
		if task.ID == id {
			return task
		}
	}
	return nil
}

func removeString(s []string, v string) []string {
	out := s[:0:0]
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}
