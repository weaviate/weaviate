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
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

// These tests use a differential oracle: the trusted baseline is loading raw WAL
// files with NO compaction (the legacy path), compared against the new on-disk
// compaction pipeline. For any input the two must produce identical graphs. The
// generator only emits realistic, unambiguous sequences; the genuinely ambiguous
// interactions have dedicated unit tests.

func quietLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.ErrorLevel)
	return l
}

type walOp func(t *testing.T, w *WALWriter)

type genState struct {
	present    map[uint64]struct{}
	tombstones map[uint64]struct{}
	levels     map[uint64]int
	// used records every ID added since the last reset. IDs are never reused in
	// production (an update is delete + insert of a fresh ID).
	used map[uint64]struct{}
}

func newGenState() *genState {
	return &genState{
		present:    map[uint64]struct{}{},
		tombstones: map[uint64]struct{}{},
		levels:     map[uint64]int{},
		used:       map[uint64]struct{}{},
	}
}

func (g *genState) presentIDs() []uint64 {
	ids := make([]uint64, 0, len(g.present))
	for id := range g.present {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (g *genState) tombstoneIDs() []uint64 {
	ids := make([]uint64, 0, len(g.tombstones))
	for id := range g.tombstones {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (g *genState) pickPresent(r *rand.Rand) (uint64, bool) {
	ids := g.presentIDs()
	if len(ids) == 0 {
		return 0, false
	}
	return ids[r.Intn(len(ids))], true
}

func (g *genState) randTargets(r *rand.Rand, max int) []uint64 {
	ids := g.presentIDs()
	if len(ids) == 0 {
		return nil
	}
	n := 1 + r.Intn(max)
	targets := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		targets = append(targets, ids[r.Intn(len(ids))])
	}
	return targets
}

// genWALOps produces n realistic operations, updating g in lock-step. When
// allowReset is false, ResetIndex is never emitted.
func genWALOps(r *rand.Rand, g *genState, n int, maxID uint64, allowReset bool) []walOp {
	ops := make([]walOp, 0, n)
	for len(ops) < n {
		switch r.Intn(22) {
		case 0, 1, 2, 3, 4: // AddNode (weighted: graphs need nodes)
			id := uint64(r.Intn(int(maxID)))
			if _, ok := g.used[id]; ok {
				continue
			}
			level := r.Intn(4)
			g.present[id] = struct{}{}
			g.used[id] = struct{}{}
			g.levels[id] = level
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddNode(id, uint16(level)))
			})
		case 5:
			id, ok := g.pickPresent(r)
			if !ok {
				continue
			}
			level := g.levels[id]
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteSetEntryPointMaxLevel(id, uint16(level)))
			})
		case 6, 7:
			src, ok := g.pickPresent(r)
			if !ok {
				continue
			}
			tgt, _ := g.pickPresent(r)
			level := r.Intn(g.levels[src] + 1)
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddLinkAtLevel(src, uint16(level), tgt))
			})
		case 8, 9:
			src, ok := g.pickPresent(r)
			if !ok {
				continue
			}
			level := r.Intn(g.levels[src] + 1)
			targets := g.randTargets(r, 5)
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddLinksAtLevel(src, uint16(level), targets))
			})
		case 10, 11:
			src, ok := g.pickPresent(r)
			if !ok {
				continue
			}
			level := r.Intn(g.levels[src] + 1)
			targets := g.randTargets(r, 6)
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteReplaceLinksAtLevel(src, uint16(level), targets))
			})
		case 12:
			id, ok := g.pickPresent(r)
			if !ok {
				continue
			}
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteClearLinks(id))
			})
		case 13:
			id, ok := g.pickPresent(r)
			if !ok {
				continue
			}
			level := r.Intn(g.levels[id] + 1)
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteClearLinksAtLevel(id, uint16(level)))
			})
		case 14, 15:
			id, ok := g.pickPresent(r)
			if !ok {
				continue
			}
			g.tombstones[id] = struct{}{}
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddTombstone(id))
			})
		case 16:
			ids := g.tombstoneIDs()
			if len(ids) == 0 {
				continue
			}
			id := ids[r.Intn(len(ids))]
			delete(g.tombstones, id)
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteRemoveTombstone(id))
			})
		case 17: // DeleteNode, never a tombstoned node (ambiguous for a differential)
			candidates := make([]uint64, 0, len(g.present))
			for _, id := range g.presentIDs() {
				if _, ts := g.tombstones[id]; !ts {
					candidates = append(candidates, id)
				}
			}
			if len(candidates) == 0 {
				continue
			}
			id := candidates[r.Intn(len(candidates))]
			delete(g.present, id)
			delete(g.levels, id)
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteDeleteNode(id))
			})
		case 18:
			data := &compression.SQData{
				A:          0.5 + float32(r.Intn(100))/100,
				B:          float32(r.Intn(100)) / 100,
				Dimensions: uint16(1 + r.Intn(256)),
			}
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddSQ(data))
			})
		case 19, 20: // more AddNode so graphs stay non-trivial
			id := uint64(r.Intn(int(maxID)))
			if _, ok := g.used[id]; ok {
				continue
			}
			level := r.Intn(4)
			g.present[id] = struct{}{}
			g.used[id] = struct{}{}
			g.levels[id] = level
			ops = append(ops, func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddNode(id, uint16(level)))
			})
		case 21: // ResetIndex, then re-seed a level-2 entrypoint so it stays non-trivial
			if !allowReset {
				continue
			}
			g.present = map[uint64]struct{}{0: {}}
			g.tombstones = map[uint64]struct{}{}
			g.levels = map[uint64]int{0: 2}
			g.used = map[uint64]struct{}{0: {}}
			ops = append(ops,
				func(t *testing.T, w *WALWriter) { require.NoError(t, w.WriteResetIndex()) },
				func(t *testing.T, w *WALWriter) { require.NoError(t, w.WriteAddNode(0, 2)) },
				func(t *testing.T, w *WALWriter) { require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 2)) },
			)
		}
	}
	return ops
}

func generateLogs(r *rand.Rand, numLogs, opsPerLog int, maxID uint64) [][]walOp {
	g := newGenState()
	logs := make([][]walOp, numLogs)

	g.present[0] = struct{}{}
	g.levels[0] = 2
	g.used[0] = struct{}{}
	seed := []walOp{
		func(t *testing.T, w *WALWriter) { require.NoError(t, w.WriteAddNode(0, 2)) },
		func(t *testing.T, w *WALWriter) { require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 2)) },
	}

	for i := 0; i < numLogs; i++ {
		// No ResetIndex in the final log, else the loaded graph could be vacuous.
		allowReset := i < numLogs-1
		ops := genWALOps(r, g, opsPerLog, maxID, allowReset)
		if i == 0 {
			ops = append(seed, ops...)
		}
		logs[i] = ops
	}
	return logs
}

func writeLogsToDir(t *testing.T, dir string, logs [][]walOp) {
	t.Helper()
	ts := int64(1000)
	for _, log := range logs {
		path := filepath.Join(dir, fmt.Sprintf("%d", ts))
		createTestWALFile(t, path, func(w *WALWriter) {
			for _, op := range log {
				op(t, w)
			}
		})
		ts += 1000
	}
}

func loadGraph(t *testing.T, dir string) *ent.DeserializationResult {
	t.Helper()
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger()})
	res, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, res, "load returned nil result for %s", dir)
	require.NotNil(t, res.State, "load returned nil state for %s", dir)
	return res.State
}

// effectivelyAbsent reports whether a node is nil or a level-0 node with no
// connections — both the legacy condensor and v2 compaction drop these.
func effectivelyAbsent(v *ent.Vertex) bool {
	if v == nil {
		return true
	}
	if v.Level != 0 {
		return false
	}
	if v.Connections == nil {
		return true
	}
	for layer := uint8(0); layer < v.Connections.Layers(); layer++ {
		if len(v.Connections.GetLayer(layer)) > 0 {
			return false
		}
	}
	return true
}

func nodeAt(res *ent.DeserializationResult, id int) *ent.Vertex {
	if id < 0 || id >= len(res.Graph.Nodes) {
		return nil
	}
	return res.Graph.Nodes[id]
}

func sameUint64Slice(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameKeySet(a, b map[uint64]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func assertConnsEqual(t *testing.T, id int, want, got *packedconn.Connections) {
	t.Helper()
	if want == nil && got == nil {
		return
	}
	require.False(t, want == nil || got == nil,
		"node %d: connections nil mismatch (want nil=%v, got nil=%v)", id, want == nil, got == nil)
	require.Equal(t, want.Layers(), got.Layers(), "node %d: layer count", id)
	for layer := uint8(0); layer < want.Layers(); layer++ {
		wl := want.GetLayer(layer)
		gl := got.GetLayer(layer)
		require.True(t, sameUint64Slice(wl, gl),
			"node %d layer %d: connections differ\n want=%v\n got =%v", id, layer, wl, gl)
	}
}

// assertGraphEqual compares the observable graph (entrypoint, per-node level and
// connections, tombstones, compression). Internal reconstruction bookkeeping is
// not compared — it legitimately differs between raw and post-compaction loads.
func assertGraphEqual(t *testing.T, want, got *ent.DeserializationResult) {
	t.Helper()

	require.Equal(t, want.Graph.EntrypointChanged, got.Graph.EntrypointChanged, "entrypointChanged")
	if want.Graph.EntrypointChanged {
		require.Equal(t, want.Graph.Entrypoint, got.Graph.Entrypoint, "entrypoint")
		require.Equal(t, want.Graph.Level, got.Graph.Level, "level")
	}

	require.True(t, sameKeySet(want.Graph.Tombstones, got.Graph.Tombstones),
		"tombstones differ\n want=%v\n got =%v", want.Graph.Tombstones, got.Graph.Tombstones)

	maxLen := len(want.Graph.Nodes)
	if len(got.Graph.Nodes) > maxLen {
		maxLen = len(got.Graph.Nodes)
	}
	for id := 0; id < maxLen; id++ {
		wn := nodeAt(want, id)
		gn := nodeAt(got, id)
		if effectivelyAbsent(wn) && effectivelyAbsent(gn) {
			continue
		}
		require.False(t, wn == nil || gn == nil,
			"node %d: presence mismatch (want nil=%v, got nil=%v)", id, wn == nil, gn == nil)
		require.Equal(t, wn.Level, gn.Level, "node %d: level", id)
		assertConnsEqual(t, id, wn.Connections, gn.Connections)
	}

	assertCompressionEqual(t, want, got)
}

func assertCompressionEqual(t *testing.T, want, got *ent.DeserializationResult) {
	t.Helper()
	require.Equal(t, want.HasCompression(), got.HasCompression(), "compressed flag")
	if !want.HasCompression() {
		return
	}
	require.Equal(t, want.Compression.SQData, got.Compression.SQData, "SQ data")
	require.Equal(t, want.Compression.PQData, got.Compression.PQData, "PQ data")
	require.Equal(t, want.Compression.RQData, got.Compression.RQData, "RQ data")
	require.Equal(t, want.Compression.BRQData, got.Compression.BRQData, "BRQ data")
}

// TestCompact_Randomized_PreservesGraphAcrossCompaction asserts that for many
// random commit streams and compaction schedules, the post-compaction load
// equals the no-compaction load.
func TestCompact_Randomized_PreservesGraphAcrossCompaction(t *testing.T) {
	seeds := []int64{1, 2, 3, 7, 42, 101, 2024, 999983, 123456789}
	thresholds := []float64{0.1, 0.5, 0.9}

	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			t.Parallel()
			r := rand.New(rand.NewSource(seed))
			logs := generateLogs(r, 6, 30, 40)

			baseDir := t.TempDir()
			writeLogsToDir(t, baseDir, logs)
			baseline := loadGraph(t, baseDir)

			require.GreaterOrEqual(t, countNonNilNodes(baseline.Graph.Nodes), 5,
				"generated graph too small to be a meaningful test")

			workDir := t.TempDir()
			writeLogsToDir(t, workDir, logs)
			compactor := NewCompactor(CompactorConfig{
				Dir:               workDir,
				MaxFilesPerMerge:  2 + r.Intn(4),
				SnapshotThreshold: thresholds[r.Intn(len(thresholds))],
				BufferSize:        DefaultBufferSize,
			}, quietLogger())

			cycles := 1 + r.Intn(8)
			for i := 0; i < cycles; i++ {
				_, err := compactor.RunCycle(nil)
				require.NoError(t, err, "RunCycle %d", i)
			}

			compacted := loadGraph(t, workDir)
			assertGraphEqual(t, baseline, compacted)
		})
	}
}

// TestCompact_ConvergesAndIdempotent pins that RunCycle reaches a fixed point
// and that the graph equals the no-compaction baseline after every cycle.
func TestCompact_ConvergesAndIdempotent(t *testing.T) {
	r := rand.New(rand.NewSource(20260617))
	logs := generateLogs(r, 8, 40, 50)

	baseDir := t.TempDir()
	writeLogsToDir(t, baseDir, logs)
	baseline := loadGraph(t, baseDir)
	require.GreaterOrEqual(t, countNonNilNodes(baseline.Graph.Nodes), 5)

	workDir := t.TempDir()
	writeLogsToDir(t, workDir, logs)
	compactor := NewCompactor(DefaultCompactorConfig(workDir), quietLogger())

	const maxCycles = 50
	converged := false
	cyclesRun := 0
	for i := 0; i < maxCycles; i++ {
		action, err := compactor.RunCycle(nil)
		require.NoError(t, err)
		cyclesRun++

		assertGraphEqual(t, baseline, loadGraph(t, workDir))

		if action == ActionNone {
			converged = true
			break
		}
	}
	require.True(t, converged, "compaction did not converge within %d cycles", maxCycles)

	// Idempotence: another cycle after convergence must be a no-op.
	action, err := compactor.RunCycle(nil)
	require.NoError(t, err)
	require.Equal(t, ActionNone, action, "extra cycle after convergence should be a no-op")
	assertGraphEqual(t, baseline, loadGraph(t, workDir))

	t.Logf("converged after %d cycles", cyclesRun)
}
