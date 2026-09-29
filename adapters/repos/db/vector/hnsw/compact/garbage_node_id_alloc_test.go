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
// commit log whose tail decodes as a commit carrying a garbage node ID. IDs
// above maxNodeID (1e11) are skipped, but that guard is far looser than
// anything that can be allocated: a garbage ID anywhere in the ~1e9..1e11
// range sails through and the node index is sized to it, i.e. tens to
// hundreds of GB of pointers. In CI that is a `fatal error: runtime: out of
// memory` (~631 GB); on a node where the mmap succeeds lazily it is a hang,
// because the next GC mark phase walks the whole pointer slice and faults
// every page in.
//
// The issue shows two allocation sites, and the scenarios cover both:
//
//   - InMemoryReader.readNode -> growIndexToAccommodateNode, reached from
//     Compactor.convertFileToSorted (the "reader" path here) and from
//     Loader.Load replaying a raw file with no snapshot present (the "loader"
//     path). growIndexToAccommodateNode has the same guard at every commit
//     type that names a node, so a garbage AddLinksAtLevel source or target
//     is covered next to AddNode.
//   - Loader.maxNodeIDInWALs -> SnapshotReader.WithMinNodes ->
//     SnapshotReader.readMetadata, reached at startup when a snapshot exists
//     and the trailing raw file carries the garbage ID (the "loader-snapshot"
//     path). The pre-scan applies the same `<= maxNodeID` filter and then
//     pre-sizes the snapshot's node slice to walMaxID+1 before a single
//     commit is applied, so a fix confined to growIndexToAccommodateNode
//     leaves this crash-loop in place. This is the frame in the issue's
//     startup stack (0x92f8000008 / 8 = walMaxID + 1).
//
// The reader and the loader must not size the node index to a garbage ID.
// That is the only contract pinned here. How the corrupt commit is handled is
// left open on purpose: skipping it (the existing maxNodeID convention),
// truncating the log at the corruption, or failing the load with a handled
// corruption error are all acceptable, so the assertions accept an error and
// require the nodes written before the corrupt commit only when a state is
// returned. The control scenarios keep the strict expectations.
//
// Not covered on purpose: how the garbage ID got into the file. The issue's
// leading theory is a reader that lost commit alignment (the ID decodes as a
// real ID shifted by three zero bytes), possibly from two writers appending
// to one same-second raw file. These scenarios write a well-formed commit
// with a garbage ID, which is what any such corruption presents to the
// reader; the misalignment and the file-name collision need their own tests.
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

	// garbageNodeIDIssue649 is the ID decoded from the raw file in the CI
	// reproduction of #649: 0x125F000000, i.e. node 0x125F (4703) with three
	// zero bytes in front of it, the shape of a commit read from the wrong
	// offset. 78.9e9 passes the maxNodeID guard and is ~588 GiB of pointers.
	garbageNodeIDIssue649 = uint64(0x125F000000)

	// garbageNodeIDPathReader is InMemoryReader.Do(nil, true) exactly as
	// Compactor.convertFileToSorted calls it (the #649 compactor stack).
	garbageNodeIDPathReader = "reader"
	// garbageNodeIDPathLoader is Loader.Load over a directory holding the log
	// as its only raw file and no snapshot: startup on a fresh shard.
	garbageNodeIDPathLoader = "loader"
	// garbageNodeIDPathLoaderSnapshot is Loader.Load over a directory holding
	// a snapshot with the real nodes and the log as the trailing raw file:
	// startup on a shard that has compacted at least once (the #649 startup
	// stack, which crash-loops the node).
	garbageNodeIDPathLoaderSnapshot = "loader-snapshot"

	// garbageNodeIDCommitAddNode puts the garbage ID in an AddNode.
	garbageNodeIDCommitAddNode = "add-node"
	// garbageNodeIDCommitLinksSource puts the garbage ID in the source of an
	// AddLinksAtLevel with real targets.
	garbageNodeIDCommitLinksSource = "links-source"
	// garbageNodeIDCommitLinksTarget puts the garbage ID in one target of an
	// AddLinksAtLevel from a real source.
	garbageNodeIDCommitLinksTarget = "links-target"
)

type garbageNodeIDScenario struct {
	name string
	// path selects the production entry point, one of the garbageNodeIDPath*
	// constants.
	path string
	// commit selects which commit carries the ID, one of the
	// garbageNodeIDCommit* constants.
	commit string
	// id is the node ID under test.
	id uint64
	// control marks a legitimately sparse ID that the reader must materialize
	// as a normal node. It passes on unfixed code and proves the child harness
	// can pass at all on that path, so a green garbage scenario is a real
	// result.
	control bool
}

var garbageNodeIDPaths = []string{
	garbageNodeIDPathReader,
	garbageNodeIDPathLoader,
	garbageNodeIDPathLoaderSnapshot,
}

var garbageNodeIDCommits = []string{
	garbageNodeIDCommitAddNode,
	garbageNodeIDCommitLinksSource,
	garbageNodeIDCommitLinksTarget,
}

var garbageNodeIDs = []struct {
	name string
	id   uint64
}{
	// Exactly at the guard boundary: the guards only reject id > maxNodeID, so
	// maxNodeID itself is the largest ID that gets through (800 GB of
	// pointers).
	{name: "id-at-maxNodeID", id: maxNodeID},
	// A single high bit set, the shape a torn record decodes to: 2^35 is
	// ~34e9, well below maxNodeID and ~275 GB of pointers.
	{name: "id-single-high-bit", id: 1 << 35},
	// The ID from the CI reproduction of the issue.
	{name: "id-from-issue-649", id: garbageNodeIDIssue649},
}

// garbageNodeIDScenarios is one control per path plus the full cross product
// of path x commit x id for the garbage cases.
var garbageNodeIDScenarios = buildGarbageNodeIDScenarios()

func buildGarbageNodeIDScenarios() []garbageNodeIDScenario {
	var out []garbageNodeIDScenario
	for _, path := range garbageNodeIDPaths {
		out = append(out, garbageNodeIDScenario{
			name:    "control/" + path + "/sparse-real-id",
			path:    path,
			commit:  garbageNodeIDCommitAddNode,
			id:      50_000,
			control: true,
		})
	}
	for _, path := range garbageNodeIDPaths {
		for _, commit := range garbageNodeIDCommits {
			for _, id := range garbageNodeIDs {
				out = append(out, garbageNodeIDScenario{
					name:   path + "/" + commit + "/" + id.name,
					path:   path,
					commit: commit,
					id:     id.id,
				})
			}
		}
	}
	return out
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

// writeGarbageNodeIDPrefix writes the real graph that precedes the scenario
// commit in a raw log: nodes 0..garbageNodeIDRealNodes-1, the entrypoint and
// links from node 0.
func writeGarbageNodeIDPrefix(t *testing.T, w *WALWriter) {
	t.Helper()
	for i := uint64(0); i < garbageNodeIDRealNodes; i++ {
		require.NoError(t, w.WriteAddNode(i, 0))
	}
	require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
	require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{1, 2, 3}))
}

// writeGarbageNodeIDSnapshot writes the same real graph as
// writeGarbageNodeIDPrefix, but as a snapshot: the starting point Loader.Load
// pre-sizes from the trailing raw files.
func writeGarbageNodeIDSnapshot(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	sw := NewSnapshotWriter(f) // default block size, matches the loader's reader
	sw.SetEntrypoint(0, 0)
	for i := uint64(0); i < garbageNodeIDRealNodes; i++ {
		conns := [][]uint64{{}}
		if i == 0 {
			conns = [][]uint64{{1, 2, 3}}
		}
		sw.AddNode(i, 0, conns, false)
	}
	require.NoError(t, sw.Flush())
	require.NoError(t, f.Close())
}

// writeGarbageNodeIDTail writes the scenario commit, then one more real node
// that a skip-and-continue reader must still apply.
func writeGarbageNodeIDTail(t *testing.T, w *WALWriter, sc garbageNodeIDScenario) {
	t.Helper()
	switch sc.commit {
	case garbageNodeIDCommitAddNode:
		require.NoError(t, w.WriteAddNode(sc.id, 0))
	case garbageNodeIDCommitLinksSource:
		require.NoError(t, w.WriteAddLinksAtLevel(sc.id, 0, []uint64{1, 2}))
	case garbageNodeIDCommitLinksTarget:
		require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{1, sc.id}))
	default:
		t.Fatalf("unknown scenario commit %q", sc.commit)
	}
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
		w := NewWALWriter(&buf)
		writeGarbageNodeIDPrefix(t, w)
		writeGarbageNodeIDTail(t, w, sc)
		reader := NewInMemoryReader(NewWALCommitReader(&buf, logger), logger)
		// keepLinkReplaceInformation=true mirrors Compactor.convertFileToSorted.
		state, err = reader.Do(nil, true)
	case garbageNodeIDPathLoader:
		dir := t.TempDir()
		f, createErr := os.Create(filepath.Join(dir, "1000"))
		require.NoError(t, createErr)
		w := NewWALWriter(f)
		writeGarbageNodeIDPrefix(t, w)
		writeGarbageNodeIDTail(t, w, sc)
		require.NoError(t, f.Close())

		var res *LoadResult
		res, err = NewLoader(LoaderConfig{Dir: dir, Logger: logger}).Load()
		if res != nil {
			state = res.State
		}
	case garbageNodeIDPathLoaderSnapshot:
		dir := t.TempDir()
		writeGarbageNodeIDSnapshot(t, filepath.Join(dir, "1000.snapshot"))
		// the raw file's timestamp is past the snapshot's, so it is in the
		// replay set that maxNodeIDInWALs pre-scans
		f, createErr := os.Create(filepath.Join(dir, "2000"))
		require.NoError(t, createErr)
		writeGarbageNodeIDTail(t, NewWALWriter(f), sc)
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

// buggySlots returns the node-slice length the unfixed code produces for the
// scenario, for the failure messages: growIndexToAccommodateNode grows to
// id+MinimumIndexGrowthDelta, the snapshot pre-size to walMaxID+1.
func (sc garbageNodeIDScenario) buggySlots() uint64 {
	if sc.path == garbageNodeIDPathLoaderSnapshot {
		return sc.id + 1
	}
	return sc.id + cache.MinimumIndexGrowthDelta
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

	buggySlots := sc.buggySlots()
	buggyGiB := float64(buggySlots) * 8 / (1 << 30)

	if ctx.Err() != nil {
		require.FailNowf(t, "child hung",
			"the %s did not return within %s while replaying an %s with ID %d: "+
				"the node index is being sized to the garbage ID (%d slots, %.0f GiB of pointers) "+
				"and GC/page-faulting on it never finishes (0-weaviate-issues#649)\n--- child output tail ---\n%s",
			sc.path, garbageNodeIDChildTimeout, sc.commit, sc.id, buggySlots, buggyGiB, tail)
	}
	require.NoError(t, runErr,
		"child exited abnormally while replaying an %s with ID %d; an "+
			"`out of memory` fatal or a kill by the OS here means the %s sized the node index to the garbage ID "+
			"(%d slots, %.0f GiB of pointers) (0-weaviate-issues#649)\n--- child output tail ---\n%s",
		sc.commit, sc.id, sc.path, buggySlots, buggyGiB, tail)

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
				"%s dropped nodes written before the corrupt %s with ID %d (%d of %d present)",
				sc.path, sc.commit, sc.id, report.RealNodesPresent, garbageNodeIDRealNodes)
		}
	}

	assert.LessOrEqualf(t, report.NodesLen, garbageNodeIDMaxSaneNodes,
		"%s sized Graph.Nodes to %d slots (%.1f GiB of pointers) for an %s with ID %d below maxNodeID; "+
			"a log with %d real nodes must not grow the index to a garbage ID (0-weaviate-issues#649)",
		sc.path, report.NodesLen, float64(report.NodesLen)*8/(1<<30), sc.commit, sc.id, garbageNodeIDRealNodes+1)
	assert.LessOrEqualf(t, report.SysDeltaBytes, uint64(garbageNodeIDMaxSaneSysBytes),
		"%s grew process memory by %.1f GiB replaying an %s with ID %d (0-weaviate-issues#649)",
		sc.path, float64(report.SysDeltaBytes)/(1<<30), sc.commit, sc.id)
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
