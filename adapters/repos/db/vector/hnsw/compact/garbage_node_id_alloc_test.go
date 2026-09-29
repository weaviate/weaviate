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

package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestInMemoryReader_GarbageNodeIDBelowMax_NoHugeIndexAlloc pins
// 0-weaviate-issues#649: a pod SIGKILLed mid async-indexing leaves a torn raw
// commit log whose tail decodes as an AddNode with a garbage ID. IDs above
// maxNodeID (1e11) are skipped by growIndexToAccommodateNode, but that guard
// is far looser than anything that can be allocated: a garbage ID anywhere in
// the ~1e9..1e11 range sails through and the reader sizes Graph.Nodes to
// id+MinimumIndexGrowthDelta pointers, i.e. tens to hundreds of GB. In CI that
// is a `fatal error: runtime: out of memory` (~631 GB); on a node where the
// mmap succeeds lazily it is a hang, because the next GC mark phase walks the
// whole pointer slice and faults every page in. Both stack traces end in
// InMemoryReader.readNode -> growIndexToAccommodateNode, reached from
// Compactor.convertFileToSorted (issue #649 and the TC-012 gate on 1.40.0-rc.1)
// and equally reachable from Loader.Load at startup, which uses the same
// reader on raw files.
//
// The reader and the loader must not size the node index to a garbage ID.
// That is the only contract pinned here. How the corrupt commit is handled is
// left open on purpose: skipping it (the existing maxNodeID convention),
// truncating the log at the corruption, or failing the load with a handled
// corruption error are all acceptable, so the assertions accept an error and
// require the nodes written before the corrupt commit only when a state is
// returned. The control scenarios keep the strict expectations.
//
// The scenarios run in a child process (this test binary re-executed with
// garbageNodeIDChildEnv set) because on the unfixed code the allocation would
// take the whole test binary down with it. The child disables GC so that, where
// the huge mmap succeeds lazily (macOS), the buggy path completes without
// touching the pages and can *report* len(Graph.Nodes) and the MemStats.Sys
// delta; the parent asserts on that report. Where the mmap is refused (Linux
// overcommit heuristics) the child dies with an OOM fatal, and where it hangs
// anyway the parent kills it at garbageNodeIDChildTimeout; both surface as a
// clean failure of the parent test naming the bug.
func TestInMemoryReader_GarbageNodeIDBelowMax_NoHugeIndexAlloc(t *testing.T) {
	if name := os.Getenv(garbageNodeIDChildEnv); name != "" {
		sc, ok := garbageNodeIDScenarioByName(name)
		require.True(t, ok, "unknown child scenario %q", name)
		runGarbageNodeIDChild(t, sc)
		return
	}

	topLevelName := strings.SplitN(t.Name(), "/", 2)[0]
	for _, sc := range garbageNodeIDScenarios {
		t.Run(sc.name, func(t *testing.T) {
			runGarbageNodeIDParent(t, topLevelName, sc)
		})
	}
}

const (
	// garbageNodeIDChildEnv carries the scenario name when this test binary is
	// re-executed as a child. Unset in the parent.
	garbageNodeIDChildEnv = "WEAVIATE_TEST_GARBAGE_NODE_ID_SCENARIO"

	// garbageNodeIDResultPrefix marks the single JSON report line the child
	// writes to stdout, so the parent can find it among the test runner output.
	garbageNodeIDResultPrefix = "GARBAGE_NODE_ID_RESULT "

	// garbageNodeIDChildTimeout bounds a child that hangs in the allocation
	// (the "GC walks 800 GB" variant of #649). The scenarios finish in well
	// under a second when the bug is absent.
	garbageNodeIDChildTimeout = 2 * time.Minute

	// garbageNodeIDMaxSaneNodes bounds len(Graph.Nodes) after replaying a log
	// whose real IDs are all below garbageNodeIDRealNodes. 2^24 slots (128 MiB
	// of pointers) is orders of magnitude above what growIndexToAccommodateNode
	// produces for the real IDs (cache.InitialSize + MinimumIndexGrowthDelta)
	// and orders of magnitude below what a garbage ID in the billions produces.
	// It is deliberately loose so that it does not pin any particular fix.
	garbageNodeIDMaxSaneNodes = 1 << 24

	// garbageNodeIDMaxSaneSysBytes bounds the MemStats.Sys growth of the child
	// across the replay (1 GiB). A garbage-sized node index alone is >= 8 GiB
	// for any ID above 1e9.
	garbageNodeIDMaxSaneSysBytes = 1 << 30

	// garbageNodeIDRealNodes is the number of real nodes written before the
	// garbage commit; one more real node is written after it.
	garbageNodeIDRealNodes = 10

	garbageNodeIDPathReader = "reader"
	garbageNodeIDPathLoader = "loader"
)

type garbageNodeIDScenario struct {
	name string
	// path selects the production entry point: "reader" is
	// InMemoryReader.Do(nil, true) exactly as Compactor.convertFileToSorted
	// calls it (the #649 stack); "loader" is Loader.Load over a directory
	// holding the log as its live raw file (startup).
	path string
	// id is the ID of the AddNode commit under test.
	id uint64
	// control marks a legitimately sparse ID that the reader must materialize
	// as a normal node. It passes on unfixed code and proves the child harness
	// can pass at all, so a green garbage scenario is a real result.
	control bool
}

var garbageNodeIDScenarios = []garbageNodeIDScenario{
	{name: "control/reader/sparse-real-id", path: garbageNodeIDPathReader, id: 50_000, control: true},
	{name: "control/loader/sparse-real-id", path: garbageNodeIDPathLoader, id: 50_000, control: true},

	// Exactly at the guard boundary: growIndexToAccommodateNode only rejects
	// id > maxNodeID, so maxNodeID itself is the largest ID that gets through
	// (800 GB of pointers).
	{name: "reader/id-at-maxNodeID", path: garbageNodeIDPathReader, id: maxNodeID},
	{name: "loader/id-at-maxNodeID", path: garbageNodeIDPathLoader, id: maxNodeID},

	// A single high bit set, the shape a torn record decodes to: 2^35 is
	// ~34e9, well below maxNodeID and ~275 GB of pointers.
	{name: "reader/id-single-high-bit", path: garbageNodeIDPathReader, id: 1 << 35},
	{name: "loader/id-single-high-bit", path: garbageNodeIDPathLoader, id: 1 << 35},
}

func garbageNodeIDScenarioByName(name string) (garbageNodeIDScenario, bool) {
	for _, sc := range garbageNodeIDScenarios {
		if sc.name == name {
			return sc, true
		}
	}
	return garbageNodeIDScenario{}, false
}

// garbageNodeIDReport is what the child measures and the parent asserts on.
type garbageNodeIDReport struct {
	// Err is the error returned by the reader/loader, empty on success.
	Err string `json:"err"`
	// NodesLen is len(Graph.Nodes) after the replay (0 if no state came back).
	NodesLen int `json:"nodes_len"`
	// SysDeltaBytes is the MemStats.Sys growth across the replay.
	SysDeltaBytes uint64 `json:"sys_delta_bytes"`
	// RealNodesPresent counts non-nil nodes among the real IDs
	// 0..garbageNodeIDRealNodes (inclusive: the last one is written after the
	// scenario commit), so it also shows whether replay continued past it.
	RealNodesPresent int `json:"real_nodes_present"`
	// StateReturned is whether the reader/loader handed back a state at all.
	StateReturned bool `json:"state_returned"`
	// ScenarioNodePresent is whether Graph.Nodes holds a node at the scenario
	// ID. Only meaningful when NodesLen covers the ID.
	ScenarioNodePresent bool `json:"scenario_node_present"`
}

// writeGarbageNodeIDLog writes real nodes, an entrypoint and links, then the
// scenario AddNode, then one more real node that a skip-and-continue reader
// must still apply.
func writeGarbageNodeIDLog(t *testing.T, w *WALWriter, sc garbageNodeIDScenario) {
	t.Helper()
	for i := uint64(0); i < garbageNodeIDRealNodes; i++ {
		require.NoError(t, w.WriteAddNode(i, 0))
	}
	require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
	require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{1, 2, 3}))
	require.NoError(t, w.WriteAddNode(sc.id, 0))
	require.NoError(t, w.WriteAddNode(garbageNodeIDRealNodes, 0))
}

func runGarbageNodeIDChild(t *testing.T, sc garbageNodeIDScenario) {
	// With GC running, the next mark phase walks the garbage-sized pointer
	// slice and the process hangs or dies: that is the bug, not the
	// measurement. With GC off the pages stay untouched and lazily mapped, so
	// the child survives long enough to report the allocation instead of
	// becoming it.
	debug.SetGCPercent(-1)

	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	var (
		state *ent.DeserializationResult
		err   error
	)
	switch sc.path {
	case garbageNodeIDPathReader:
		var buf bytes.Buffer
		writeGarbageNodeIDLog(t, NewWALWriter(&buf), sc)
		reader := NewInMemoryReader(NewWALCommitReader(&buf, logger), logger)
		// keepLinkReplaceInformation=true mirrors Compactor.convertFileToSorted.
		state, err = reader.Do(nil, true)
	case garbageNodeIDPathLoader:
		dir := t.TempDir()
		f, createErr := os.Create(filepath.Join(dir, "1000"))
		require.NoError(t, createErr)
		writeGarbageNodeIDLog(t, NewWALWriter(f), sc)
		require.NoError(t, f.Close())

		var res *LoadResult
		res, err = NewLoader(LoaderConfig{Dir: dir, Logger: logger}).Load()
		if res != nil {
			state = res.State
		}
	default:
		t.Fatalf("unknown scenario path %q", sc.path)
	}

	runtime.ReadMemStats(&after)

	report := garbageNodeIDReport{SysDeltaBytes: after.Sys - before.Sys}
	if err != nil {
		report.Err = err.Error()
	}
	if state != nil {
		report.StateReturned = true
		nodes := state.Graph.Nodes
		report.NodesLen = len(nodes)
		for id := 0; id <= garbageNodeIDRealNodes && id < len(nodes); id++ {
			if nodes[id] != nil {
				report.RealNodesPresent++
			}
		}
		if sc.id < uint64(len(nodes)) {
			report.ScenarioNodePresent = nodes[sc.id] != nil
		}
	}

	line, marshalErr := json.Marshal(report)
	require.NoError(t, marshalErr)
	_, writeErr := os.Stdout.WriteString(garbageNodeIDResultPrefix + string(line) + "\n")
	require.NoError(t, writeErr)
}

func runGarbageNodeIDParent(t *testing.T, topLevelName string, sc garbageNodeIDScenario) {
	exe, err := os.Executable()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), garbageNodeIDChildTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, exe, "-test.run=^"+topLevelName+"$", "-test.count=1")
	cmd.Env = append(os.Environ(), garbageNodeIDChildEnv+"="+sc.name)
	cmd.WaitDelay = 10 * time.Second
	out, runErr := cmd.CombinedOutput()
	tail := lastLines(string(out), 40)

	buggySlots := sc.id + cache.MinimumIndexGrowthDelta
	buggyGiB := float64(buggySlots) * 8 / (1 << 30)

	if ctx.Err() != nil {
		require.FailNowf(t, "child hung",
			"the %s did not return within %s while replaying an AddNode with ID %d: "+
				"the node index is being sized to the garbage ID (%d slots, %.0f GiB of pointers) "+
				"and GC/page-faulting on it never finishes (0-weaviate-issues#649)\n--- child output tail ---\n%s",
			sc.path, garbageNodeIDChildTimeout, sc.id, buggySlots, buggyGiB, tail)
	}
	require.NoError(t, runErr,
		"child exited abnormally while replaying an AddNode with ID %d; an "+
			"`out of memory` fatal here means the %s sized the node index to the garbage ID "+
			"(%d slots, %.0f GiB of pointers) (0-weaviate-issues#649)\n--- child output tail ---\n%s",
		sc.id, sc.path, buggySlots, buggyGiB, tail)

	report, found := parseGarbageNodeIDReport(string(out))
	require.True(t, found, "child produced no %q line\n--- child output tail ---\n%s",
		strings.TrimSpace(garbageNodeIDResultPrefix), tail)

	if sc.control {
		assert.Empty(t, report.Err, "%s returned an error for a legitimately sparse ID", sc.path)
		assert.Equal(t, garbageNodeIDRealNodes+1, report.RealNodesPresent,
			"all real nodes must be applied around a legitimately sparse ID %d", sc.id)
		assert.True(t, report.ScenarioNodePresent,
			"a legitimately sparse ID %d must be materialized as a normal node (nodes_len=%d)", sc.id, report.NodesLen)
	} else {
		// a handled corruption error is one acceptable outcome; a returned
		// state must still hold every node written before the corrupt commit
		if report.Err != "" {
			t.Logf("%s reported the corrupt commit as an error: %s", sc.path, report.Err)
		}
		if report.StateReturned {
			assert.GreaterOrEqualf(t, report.RealNodesPresent, garbageNodeIDRealNodes,
				"%s dropped nodes written before the corrupt AddNode with ID %d (%d of %d present)",
				sc.path, sc.id, report.RealNodesPresent, garbageNodeIDRealNodes)
		}
	}

	assert.LessOrEqualf(t, report.NodesLen, garbageNodeIDMaxSaneNodes,
		"%s sized Graph.Nodes to %d slots (%.1f GiB of pointers) for an AddNode with ID %d below maxNodeID; "+
			"a raw log with %d real nodes must not grow the index to a garbage ID (0-weaviate-issues#649)",
		sc.path, report.NodesLen, float64(report.NodesLen)*8/(1<<30), sc.id, garbageNodeIDRealNodes+1)
	assert.LessOrEqualf(t, report.SysDeltaBytes, uint64(garbageNodeIDMaxSaneSysBytes),
		"%s grew process memory by %.1f GiB replaying an AddNode with ID %d (0-weaviate-issues#649)",
		sc.path, float64(report.SysDeltaBytes)/(1<<30), sc.id)
}

func parseGarbageNodeIDReport(out string) (garbageNodeIDReport, bool) {
	for _, line := range strings.Split(out, "\n") {
		rest, ok := strings.CutPrefix(line, garbageNodeIDResultPrefix)
		if !ok {
			continue
		}
		var report garbageNodeIDReport
		if err := json.Unmarshal([]byte(rest), &report); err != nil {
			return garbageNodeIDReport{Err: fmt.Sprintf("unparseable child report %q: %v", rest, err)}, true
		}
		return report, true
	}
	return garbageNodeIDReport{}, false
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
