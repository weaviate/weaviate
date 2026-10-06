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
	"encoding/binary"
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
//     paths). growIndexToAccommodateNode has the same guard at every commit
//     type that names a node, so a garbage AddLinksAtLevel source or target
//     is covered next to AddNode.
//   - Loader.maxNodeIDInWALs -> SnapshotReader.WithMinNodes ->
//     SnapshotReader.readMetadata, reached at startup when a snapshot exists
//     and the trailing raw file carries the garbage ID (the "loader-snapshot"
//     paths). The pre-scan applies the same `<= maxNodeID` filter and then
//     pre-sizes the snapshot's node slice to walMaxID+1 before a single
//     commit is applied, so a fix confined to growIndexToAccommodateNode
//     leaves this crash-loop in place. This is the frame in the issue's
//     startup stack (0x92f8000008 / 8 = walMaxID + 1).
//
// #13377 bounds the startup load for indexes whose node IDs are document IDs:
// the loader reads the shard's `indexcount` counter and rejects any record
// naming a node above counter+2^24, truncating the file before it. The
// "loader" and "loader-snapshot" paths are wired like those indexes (a counter
// next to the commit log directory, NodeIDsAreDocIDs set) and pass with it.
// Two production configurations still have no bound and stay red here:
//
//   - The compactor ("reader"). #13377 gives it no limit on the premise that
//     it only reads files the loader already checked or the running process
//     wrote. The issue's first stack is this path, on a raw file the running
//     process wrote after the restart.
//   - Startup without a limit ("loader-nolimit", "loader-snapshot-nolimit"):
//     multivector indexes without Muvera, HFresh's centroid index, and a shard
//     whose counter is 0 or unreadable all load with NodeIDsAreDocIDs unset or
//     no usable counter, which #13377 leaves at today's behavior.
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
// clean failure of the parent test naming the bug. For the fatal, the parent
// keeps the head of the runtime dump and requires the `out of memory` line
// plus the sizing function on the crashing goroutine, so an unrelated panic
// in the child (also exit status 2) fails as an unrelated panic, not as a
// confirmation of this bug.
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

	// garbageNodeIDChildTimeout bounds a child that neither reports nor dies:
	// the OS tearing down the garbage-sized mapping, or GC/page-faulting that
	// never finishes (the "GC walks 800 GB" variant of #649). The scenarios
	// finish in well under a second when the bug is absent, and the children
	// run serially, so the bound is kept short enough for the whole matrix to
	// fail within the default package timeout when every scenario hangs.
	garbageNodeIDChildTimeout = 10 * time.Second

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
	// as its only raw file and no snapshot: startup on a fresh shard. Wired
	// like production for an index whose node IDs are document IDs: the
	// commit log directory sits in a shard directory with an `indexcount`
	// counter of garbageNodeIDDocIDCounter, and NodeIDsAreDocIDs is set.
	garbageNodeIDPathLoader = "loader"
	// garbageNodeIDPathLoaderSnapshot is Loader.Load over a directory holding
	// a snapshot with the real nodes and the log as the trailing raw file:
	// startup on a shard that has compacted at least once (the #649 startup
	// stack, which crash-loops the node). Wired like garbageNodeIDPathLoader.
	garbageNodeIDPathLoaderSnapshot = "loader-snapshot"
	// garbageNodeIDPathLoaderNoLimit is garbageNodeIDPathLoader with
	// NodeIDsAreDocIDs unset: a multivector index without Muvera, HFresh's
	// centroid index, or any shard whose counter the loader cannot use.
	garbageNodeIDPathLoaderNoLimit = "loader-nolimit"
	// garbageNodeIDPathLoaderSnapshotNoLimit is garbageNodeIDPathLoaderSnapshot
	// with NodeIDsAreDocIDs unset.
	garbageNodeIDPathLoaderSnapshotNoLimit = "loader-snapshot-nolimit"

	// garbageNodeIDDocIDCounter is the shard's document-ID counter for the
	// bounded loader paths: one past the highest real node, as the shard's
	// counter would be after handing out those IDs.
	garbageNodeIDDocIDCounter = uint64(garbageNodeIDRealNodes + 1)

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
	garbageNodeIDPathLoaderNoLimit,
	garbageNodeIDPathLoaderSnapshotNoLimit,
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
	// PrefixNodesPresent counts non-nil nodes among the real IDs
	// 0..garbageNodeIDRealNodes-1, all written before the scenario commit.
	PrefixNodesPresent int `json:"prefix_nodes_present"`
	// TailNodePresent is whether the real node written after the scenario
	// commit (ID garbageNodeIDRealNodes) is present, i.e. whether replay
	// continued past the commit.
	TailNodePresent bool `json:"tail_node_present"`
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

// garbageNodeIDCommitLogDir returns a commit log directory inside a shard
// directory, with the shard's `indexcount` counter written next to it the way
// the shard persists it (8 bytes little-endian), so the loader can bound node
// IDs for the paths that opt in.
func garbageNodeIDCommitLogDir(t *testing.T) string {
	t.Helper()
	shardDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"),
		binary.LittleEndian.AppendUint64(nil, garbageNodeIDDocIDCounter), 0o644))
	dir := filepath.Join(shardDir, "main.hnsw.commitlog.d")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	return dir
}

// usesSnapshot reports whether the path starts from a snapshot with the real
// nodes and replays only the tail from a raw file.
func (sc garbageNodeIDScenario) usesSnapshot() bool {
	return sc.path == garbageNodeIDPathLoaderSnapshot || sc.path == garbageNodeIDPathLoaderSnapshotNoLimit
}

// nodeIDsAreDocIDs reports whether the loader is told node IDs are document
// IDs, which is what lets it bound them by the shard's counter (#13377).
func (sc garbageNodeIDScenario) nodeIDsAreDocIDs() bool {
	return sc.path == garbageNodeIDPathLoader || sc.path == garbageNodeIDPathLoaderSnapshot
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
	case garbageNodeIDPathLoader, garbageNodeIDPathLoaderNoLimit:
		dir := garbageNodeIDCommitLogDir(t)
		f, createErr := os.Create(filepath.Join(dir, "1000"))
		require.NoError(t, createErr)
		w := NewWALWriter(f)
		writeGarbageNodeIDPrefix(t, w)
		writeGarbageNodeIDTail(t, w, sc)
		require.NoError(t, f.Close())

		var res *LoadResult
		res, err = NewLoader(LoaderConfig{Dir: dir, Logger: logger, NodeIDsAreDocIDs: sc.nodeIDsAreDocIDs()}).Load()
		if res != nil {
			state = res.State
		}
	case garbageNodeIDPathLoaderSnapshot, garbageNodeIDPathLoaderSnapshotNoLimit:
		dir := garbageNodeIDCommitLogDir(t)
		writeGarbageNodeIDSnapshot(t, filepath.Join(dir, "1000.snapshot"))
		// the raw file's timestamp is past the snapshot's, so it is in the
		// replay set that maxNodeIDInWALs pre-scans
		f, createErr := os.Create(filepath.Join(dir, "2000"))
		require.NoError(t, createErr)
		writeGarbageNodeIDTail(t, NewWALWriter(f), sc)
		require.NoError(t, f.Close())

		var res *LoadResult
		res, err = NewLoader(LoaderConfig{Dir: dir, Logger: logger, NodeIDsAreDocIDs: sc.nodeIDsAreDocIDs()}).Load()
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
		for id := 0; id < garbageNodeIDRealNodes && id < len(nodes); id++ {
			if nodes[id] != nil {
				report.PrefixNodesPresent++
			}
		}
		if garbageNodeIDRealNodes < len(nodes) {
			report.TailNodePresent = nodes[garbageNodeIDRealNodes] != nil
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
	if sc.usesSnapshot() {
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
				"and the OS or GC/page-faulting never finishes with it (0-weaviate-issues#649)\n--- child output tail ---\n%s",
			sc.path, garbageNodeIDChildTimeout, sc.commit, sc.id, buggySlots, buggyGiB, tail)
	}
	if runErr != nil {
		// Exit status 2 is what the Go runtime uses for every fatal error, so
		// the exit code alone cannot tell the #649 allocation from an
		// unrelated panic. Keep the head of the dump, where the runtime names
		// the error and prints the crashing goroutine, and require both to
		// match this bug before reporting it as this bug.
		crash, hasDump := parseChildCrash(string(out))
		if !hasDump {
			require.FailNowf(t, "child died without a runtime dump",
				"child exited with %v while replaying an %s with ID %d and left no `fatal error:` or `panic:` line; "+
					"a kill by the OS is consistent with the %s sizing the node index to the garbage ID "+
					"(%d slots, %.0f GiB of pointers), but cannot be told apart from an unrelated death "+
					"(0-weaviate-issues#649)\n--- child output tail ---\n%s",
				runErr, sc.commit, sc.id, sc.path, buggySlots, buggyGiB, tail)
		}
		frame := sc.allocFrame()
		require.Truef(t, strings.Contains(crash.fatalLine, "out of memory") && strings.Contains(crash.running, frame),
			"child crashed (%v) for a reason other than the #649 allocation while replaying an %s with ID %d: "+
				"expected `fatal error: runtime: out of memory` with %s on the crashing goroutine\n--- child crash ---\n%s",
			runErr, sc.commit, sc.id, frame, crash.running)
		require.FailNowf(t, "child ran out of memory sizing the node index",
			"%s: the %s sized the node index to the garbage ID (%d slots, %.0f GiB of pointers) "+
				"replaying an %s with ID %d (0-weaviate-issues#649)\n--- child crash ---\n%s",
			crash.fatalLine, sc.path, buggySlots, buggyGiB, sc.commit, sc.id, crash.running)
	}

	report, found := parseGarbageNodeIDReport(string(out))
	require.True(t, found, "child produced no %q line\n--- child output tail ---\n%s",
		strings.TrimSpace(garbageNodeIDResultPrefix), tail)

	if sc.control {
		assert.Empty(t, report.Err, "%s returned an error for a legitimately sparse ID", sc.path)
		assert.Equal(t, garbageNodeIDRealNodes, report.PrefixNodesPresent,
			"all real nodes before a legitimately sparse ID %d must be applied", sc.id)
		assert.True(t, report.TailNodePresent,
			"the real node after a legitimately sparse ID %d must be applied", sc.id)
		assert.True(t, report.ScenarioNodePresent,
			"a legitimately sparse ID %d must be materialized as a normal node (nodes_len=%d)", sc.id, report.NodesLen)
	} else {
		// a handled corruption error is one acceptable outcome; a returned
		// state must still hold every node written before the corrupt commit.
		// No state and no error is not: the loader returns (nil, nil) for an
		// empty result, and this fixture is not empty, so that would mean the
		// valid prefix was dropped silently.
		if report.Err != "" {
			t.Logf("%s reported the corrupt commit as an error: %s", sc.path, report.Err)
		}
		assert.Truef(t, report.StateReturned || report.Err != "",
			"%s returned neither a state nor an error for a log with %d real nodes before the corrupt %s with ID %d",
			sc.path, garbageNodeIDRealNodes, sc.commit, sc.id)
		if report.StateReturned {
			assert.Equalf(t, garbageNodeIDRealNodes, report.PrefixNodesPresent,
				"%s dropped nodes written before the corrupt %s with ID %d (%d of %d present)",
				sc.path, sc.commit, sc.id, report.PrefixNodesPresent, garbageNodeIDRealNodes)
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

// allocFrame returns the function that sizes the node index on the unfixed
// code for this scenario's path. A child OOM fatal must have it on the
// crashing goroutine's stack to count as #649: growIndexToAccommodateNode for
// the raw-file replay, SnapshotReader.readMetadata for the snapshot pre-size.
func (sc garbageNodeIDScenario) allocFrame() string {
	if sc.usesSnapshot() {
		return "(*SnapshotReader).readMetadata"
	}
	return "growIndexToAccommodateNode"
}

// childCrash is the head of a Go runtime crash dump.
type childCrash struct {
	// fatalLine is the `fatal error: ...` or `panic: ...` line.
	fatalLine string
	// running is the dump from fatalLine through the end of the first
	// `goroutine N [running]:` block: the runtime stack and the crashing
	// goroutine, without the idle goroutines that follow and that a tail
	// excerpt would show instead.
	running string
}

// garbageNodeIDCrashMaxLines caps the excerpt kept from a child crash dump.
const garbageNodeIDCrashMaxLines = 80

// parseChildCrash finds the first `fatal error:` or `panic:` line in the
// child's combined output and returns it with the crashing goroutine's stack.
// The bool is false when the output holds no runtime dump, which is what a
// child killed by a signal leaves behind.
func parseChildCrash(out string) (childCrash, bool) {
	lines := strings.Split(out, "\n")
	start := -1
	for i, l := range lines {
		if strings.HasPrefix(l, "fatal error: ") || strings.HasPrefix(l, "panic: ") {
			start = i
			break
		}
	}
	if start < 0 {
		return childCrash{}, false
	}
	end := len(lines)
	inRunning := false
	for i := start + 1; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "goroutine ") && strings.Contains(lines[i], "[running]") {
			inRunning = true
			continue
		}
		if inRunning && strings.TrimSpace(lines[i]) == "" {
			end = i
			break
		}
	}
	if end-start > garbageNodeIDCrashMaxLines {
		end = start + garbageNodeIDCrashMaxLines
	}
	return childCrash{fatalLine: lines[start], running: strings.Join(lines[start:end], "\n")}, true
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

// TestParseChildCrash covers the crash-dump excerpt the parent keeps from a
// child that died with a runtime fatal, on every platform, since only Linux
// with overcommit refused produces the real thing.
func TestParseChildCrash(t *testing.T) {
	oomDump := strings.Join([]string{
		"=== RUN   TestInMemoryReader_GarbageNodeIDBelowMax_NoHugeIndexAlloc",
		"fatal error: runtime: out of memory",
		"",
		"runtime stack:",
		"runtime.throw({0x1234?, 0x0?})",
		"\t/usr/local/go/src/runtime/panic.go:1101 +0x48 fp=0x0 sp=0x0 pc=0x0",
		"",
		"goroutine 7 gp=0xc000007000 m=0 mp=0x1 [running]:",
		"runtime.systemstack_switch()",
		"\t/usr/local/go/src/runtime/asm_arm64.s:200 +0x8 fp=0x0 sp=0x0 pc=0x0",
		"runtime.mallocgc(0x92f8003e80, 0x1, 0x1)",
		"\t/usr/local/go/src/runtime/malloc.go:1050 +0x0",
		"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact.growIndexToAccommodateNode(...)",
		"\t/src/adapters/repos/db/vector/hnsw/compact/in_memory_reader.go:573",
		"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact.(*InMemoryReader).readNode(...)",
		"\t/src/adapters/repos/db/vector/hnsw/compact/in_memory_reader.go:223",
		"",
		"goroutine 1 gp=0xc000006000 m=nil [chan receive]:",
		"runtime.gopark(...)",
		"",
		"goroutine 18 gp=0xc000007380 m=nil [GC worker (idle)]:",
		"runtime.gopark(...)",
		"exit status 2",
	}, "\n")

	panicDump := strings.Join([]string{
		"panic: unrelated boom",
		"",
		"goroutine 7 [running]:",
		"testing.tRunner.func1.2(...)",
		"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact.runGarbageNodeIDChild(...)",
		"",
		"goroutine 1 [chan receive]:",
		"runtime.gopark(...)",
	}, "\n")

	cases := []struct {
		name        string
		out         string
		wantFound   bool
		wantFatal   string
		wantInStack []string
		wantOutside []string
	}{
		{
			name:        "oom fatal keeps the runtime stack and the crashing goroutine only",
			out:         oomDump,
			wantFound:   true,
			wantFatal:   "fatal error: runtime: out of memory",
			wantInStack: []string{"runtime stack:", "[running]", "runtime.mallocgc(0x92f8003e80", "compact.growIndexToAccommodateNode", "(*InMemoryReader).readNode"},
			wantOutside: []string{"=== RUN", "[chan receive]", "GC worker (idle)", "exit status 2"},
		},
		{
			name:        "panic keeps the crashing goroutine only",
			out:         panicDump,
			wantFound:   true,
			wantFatal:   "panic: unrelated boom",
			wantInStack: []string{"[running]", "runGarbageNodeIDChild"},
			wantOutside: []string{"[chan receive]"},
		},
		{
			name:      "no dump (killed by a signal)",
			out:       "=== RUN   TestInMemoryReader_GarbageNodeIDBelowMax_NoHugeIndexAlloc\n",
			wantFound: false,
		},
		{
			name:      "empty output",
			out:       "",
			wantFound: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			crash, found := parseChildCrash(tc.out)
			require.Equal(t, tc.wantFound, found)
			if !tc.wantFound {
				return
			}
			assert.Equal(t, tc.wantFatal, crash.fatalLine)
			for _, s := range tc.wantInStack {
				assert.Contains(t, crash.running, s)
			}
			for _, s := range tc.wantOutside {
				assert.NotContains(t, crash.running, s)
			}
		})
	}
}
