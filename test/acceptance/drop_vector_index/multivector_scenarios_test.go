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

package drop_vector_index

import (
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// multiVectorVariant is one multi-vector encoding under test. The four differ
// only in which extra on-disk state they carry, which is exactly what the drop
// has to clean up: BQ adds the compressed bucket, muvera adds a bucket of its
// own ("<indexID>_muvera_vectors", see hnsw.New).
type multiVectorVariant struct {
	name   string
	bq     bool
	muvera bool
}

func (v multiVectorVariant) config() models.VectorConfig {
	multivector := map[string]any{
		"enabled":     true,
		"aggregation": "maxSim",
	}
	if v.muvera {
		multivector["muvera"] = map[string]any{
			"enabled":      true,
			"ksim":         4,
			"dprojections": 16,
			"repetitions":  10,
		}
	}
	idxCfg := map[string]any{"multivector": multivector}
	if v.bq {
		idxCfg["bq"] = map[string]any{"enabled": true}
	}
	return models.VectorConfig{
		Vectorizer:        map[string]any{"none": map[string]any{}},
		VectorIndexType:   "hnsw",
		VectorIndexConfig: idxCfg,
	}
}

// testMultiVectorLeavesNoFilesOnDisk pins that dropping a multi-vector index
// removes every directory it owns, across all four encodings.
//
// The muvera variants are the reason this exists: a muvera-encoded index keeps
// its encoded vectors in a bucket of its own that the drop used to miss
// entirely, so the encoded copy of every vector survived — unreachable and
// uncollectable, because once the drop completes the vector's schema entry is
// gone and the startup sweep never looks at it again.
//
// The variants are dropped ONE AT A TIME, and after each drop the survivors are
// re-checked. Their names deliberately share prefixes ("multivector" is a
// prefix of "multivector_bq"), so a cleanup matching loosely would take a live
// index's storage with it — a worse bug than the one being fixed.
func testMultiVectorLeavesNoFilesOnDisk(compose *docker.DockerCompose) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		const (
			className = "DropVectorIndexMultiVectorDisk"
			sibling   = "sibling"
			dim       = 32
			tokens    = 4
			count     = 50
		)

		variants := []multiVectorVariant{
			{name: "multivector_muvera_bq", bq: true, muvera: true},
			{name: "multivector_bq", bq: true},
			{name: "multivector_muvera", muvera: true},
			{name: "multivector"},
		}

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create every encoding and fill them", func(t *testing.T) {
			vectorConfig := map[string]models.VectorConfig{
				// Keeps the collection from going vectorless once all four
				// multi-vectors are dropped, which is its own path.
				sibling: noneVectorConfig(),
			}
			for _, v := range variants {
				vectorConfig[v.name] = v.config()
			}
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: vectorConfig,
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			// The configs are deliberately NOT re-read here. Two later checks
			// pin the same thing more strongly: the batch below is only
			// accepted as [][]float32 if multivector really stuck, and the
			// muvera variants' on-disk precondition fails if muvera did not.
			// Re-reading the flags we just sent would only move the failure
			// slightly earlier.

			batch := make([]*models.Object, count)
			for i := range count {
				vectors := models.Vectors{sibling: randVec(dim, float32(i))}
				for _, v := range variants {
					vecs := make([][]float32, tokens)
					for tk := range tokens {
						vecs[tk] = randVec(dim, float32(i*10+tk))
					}
					vectors[v.name] = vecs
				}
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000031%02d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors:    vectors,
				}
			}
			helper.CreateObjectsBatch(t, batch)
			time.Sleep(5 * time.Second) // past the 1s dirty-flush, so segments land on disk
		})

		// Snapshot what each variant owns before anything is dropped, so the
		// assertions below compare against observed state rather than a list of
		// paths guessed in advance.
		owned := map[string][]string{}
		inventoried := t.Run("every encoding owns directories on disk", func(t *testing.T) {
			all := multiVectorDirsOnEveryNode(ctx, t, compose)
			for _, v := range variants {
				owned[v.name] = dirsOwnedBy(all, v.name)
				require.NotEmpty(t, owned[v.name],
					"precondition: %s must have on-disk state, or dropping it proves nothing", v.name)
				t.Logf("%s owns:\n  %s", v.name, strings.Join(owned[v.name], "\n  "))

				if v.muvera {
					require.True(t, hasSuffix(owned[v.name], "_muvera_vectors"),
						"precondition: %s must have created its muvera bucket, got %v", v.name, owned[v.name])
				} else {
					require.False(t, hasSuffix(owned[v.name], "_muvera_vectors"),
						"%s has muvera disabled and must NOT have a muvera bucket, got %v", v.name, owned[v.name])
					// Without this, NotEmpty above is satisfied by the raw
					// bucket and the commit log alone, and the post-drop
					// Empty would pass over a bucket that never existed.
					require.True(t, hasSuffix(owned[v.name], "_mv_mappings"),
						"precondition: %s must have created its mappings bucket, got %v", v.name, owned[v.name])
				}
			}
		})

		// Without the snapshot above, the drop subtests assert against nil
		// `owned` entries: one real precondition failure would surface as five,
		// four of them bogus "drop disturbed a sibling" reports pointing at the
		// wrong thing. t.Run returns false when its subtest failed.
		if !inventoried {
			t.Fatal("skipping the drops: the pre-drop inventory failed, so nothing below can be trusted")
		}

		// Dropped one at a time so each drop can be checked against the indexes
		// still standing.
		for i, v := range variants {
			t.Run("drop "+v.name, func(t *testing.T) {
				dropTargetVector(t, className, v.name)
				eventuallyTargetVectorRemoved(t, className, v.name)
				waitForNoActiveDropTask(t)

				all := multiVectorDirsOnEveryNode(ctx, t, compose)

				left := dirsOwnedBy(all, v.name)
				for _, dir := range left {
					t.Logf("SURVIVED: %s", dir)
				}
				require.Empty(t, left,
					"dropping %s must leave none of its on-disk state, but these survived:\n  %s",
					v.name, strings.Join(left, "\n  "))

				unlisted := dirsNamedAfter(all, v.name)
				require.Empty(t, unlisted,
					"dropping %s must leave no directory named after it, including ones the artifact list does not know about:\n  %s",
					v.name, strings.Join(unlisted, "\n  "))

				for _, other := range variants[i+1:] {
					require.Equal(t, owned[other.name], dirsOwnedBy(all, other.name),
						"dropping %s must not disturb %s, whose name shares its prefix",
						v.name, other.name)
				}
			})
		}
	}
}

// dirsOwnedBy returns the directories belonging to targetVector, matched on the
// EXACT names its index builds. Matching by substring would be wrong here: the
// variants' names are prefixes of one another, so "multivector" would claim
// every other variant's directories too.
func dirsOwnedBy(allDirs []string, targetVector string) []string {
	// Read from the same helper the cleanup uses, so a newly added artifact
	// cannot be cleaned-but-unasserted or asserted-but-uncleaned. A hand-copied
	// list here is how "_mv_mappings" came to be missing from this filter while
	// the bucket leaked in the container and the test still reported clean.
	names := map[string]struct{}{}
	for _, n := range helpers.VectorIndexArtifactsFor(targetVector, nil).All() {
		names[n] = struct{}{}
	}
	var out []string
	for _, dir := range allDirs {
		if _, ok := names[path.Base(dir)]; ok {
			out = append(out, dir)
		}
	}
	sort.Strings(out)
	return out
}

// dirsNamedAfter returns every observed directory whose name is derived from
// targetVector, WITHOUT going through the artifact list. dirsOwnedBy filters
// the walk down to the names the cleanup already knows about, so an artifact
// the index creates that nobody added to helpers.VectorIndexArtifactsFor is
// discarded before the leak assertion sees it — exactly how "_mv_mappings"
// leaked while this test reported clean. This is the unfiltered net.
//
// Prefix matching is safe here only because the variants are dropped longest
// name first: by the time v.name is dropped, every variant whose name extends
// it is already gone.
func dirsNamedAfter(allDirs []string, targetVector string) []string {
	var out []string
	for _, dir := range allDirs {
		base := path.Base(dir)
		if strings.HasPrefix(base, "vectors_"+targetVector) ||
			strings.HasPrefix(base, "vectors_compressed_"+targetVector) {
			out = append(out, dir)
		}
	}
	sort.Strings(out)
	return out
}

// multiVectorDirsOnEveryNode collects candidate directories from EVERY node.
// The class's shard lives on whichever node the hash lands it on, so checking
// only the node the client happens to talk to would pass on the other two by
// looking in the wrong place.
//
// Searched from / with -xdev rather than a hardcoded PERSISTENCE_DATA_PATH (so
// /proc and /sys are skipped): a path that moved would otherwise make the
// post-drop assertions pass vacuously by finding nothing.
func multiVectorDirsOnEveryNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose) []string {
	t.Helper()
	var dirs []string
	for n := 1; n <= 3; n++ {
		node := compose.GetWeaviateNode(n)
		if node == nil {
			continue
		}
		for _, line := range strings.Split(findVectorDirs(ctx, t, node.Container()), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				dirs = append(dirs, node.Name()+":"+line)
			}
		}
	}
	return dirs
}

// dataRoots caches the discovered data directory per container, so the walk
// below is scoped to it instead of crossing the whole filesystem on every one
// of the ~10 calls a run makes.
var (
	dataRootsMu sync.Mutex
	dataRoots   = map[string]string{}
)

// dataRootOf locates PERSISTENCE_DATA_PATH inside the container once. Resolved
// at runtime rather than hardcoded: a path that moved would otherwise make
// every post-drop assertion pass vacuously by finding nothing.
func dataRootOf(ctx context.Context, t *testing.T, c testcontainers.Container) string {
	t.Helper()
	dataRootsMu.Lock()
	defer dataRootsMu.Unlock()
	if root, ok := dataRoots[c.GetContainerID()]; ok {
		return root
	}

	// Located by finding a shard's "lsm" directory at ANY depth and walking up
	// three levels (<root>/<class>/<shard>/lsm). Depth is not assumed: with the
	// default PERSISTENCE_DATA_PATH the marker sits at depth 4, and a bounded
	// search that guessed wrong found nothing at all. `head -1` stops the walk
	// at the first hit, so this stays cheap despite being unbounded, and it
	// runs once per container.
	out, _ := execInContainer(ctx, t, c, []string{
		"sh", "-c", "find / -xdev -type d -name lsm 2>/dev/null | head -1",
	})
	root := ""
	for _, line := range strings.Split(out, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			// <root>/<class>/<shard>/lsm — three levels up, not two. Stopping
			// at the class directory scopes every later search to whichever
			// class happened to be found first, and the result is CACHED, so
			// one test passes and the next finds nothing.
			root = path.Dir(path.Dir(path.Dir(line)))
			break
		}
	}
	require.NotEmpty(t, root,
		"could not locate the data root in the container; a scoped search would find nothing and pass vacuously")
	// Logged once per container: if the root is ever wrong again, the failure
	// says which directory was searched instead of only that nothing was found.
	t.Logf("data root in %s resolved to %s", c.GetContainerID()[:12], root)
	dataRoots[c.GetContainerID()] = root
	return root
}

// findVectorDirs lists candidate directories under the data root.
//
// find's exit code is deliberately NOT asserted. BusyBox find lstats every
// entry it reads and exits 1 when one vanishes between readdir and lstat —
// which segment flushes and compactions make routine here — while still
// printing every real match. Failing on that would flake a run with zero
// leaks. A genuinely broken search shows up instead as an empty result, which
// the precondition subtest turns into a loud failure.
func findVectorDirs(ctx context.Context, t *testing.T, c testcontainers.Container) string {
	t.Helper()
	out, _ := execInContainer(ctx, t, c, []string{
		"find", dataRootOf(ctx, t, c), "-xdev", "-type", "d",
		"(", "-name", "vectors_*", "-o", "-name", "hfresh_*", ")",
	})
	return out
}

func hasSuffix(paths []string, suffix string) bool {
	for _, p := range paths {
		if strings.HasSuffix(p, suffix) {
			return true
		}
	}
	return false
}

// execInContainer runs cmd and returns its stdout and exit code. The code is
// returned rather than asserted: callers decide, because for `find` a non-zero
// exit is routine (see findVectorDirs) while for other commands it is not.
func execInContainer(ctx context.Context, t *testing.T, c testcontainers.Container, cmd []string) (string, int) {
	t.Helper()
	code, reader, err := c.Exec(ctx, cmd, tcexec.Multiplexed())
	require.NoError(t, err, "exec %v", cmd)
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	require.NoError(t, err)
	return buf.String(), code
}
