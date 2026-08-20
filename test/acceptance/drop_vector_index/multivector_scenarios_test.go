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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

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

			// Confirm each config survived the round trip: a variant that
			// silently lost its multivector or muvera flag would create less
			// on-disk state and make its assertions vacuous.
			got := helper.GetClass(t, className)
			for _, v := range variants {
				cfg, ok := got.VectorConfig[v.name].VectorIndexConfig.(map[string]any)
				require.True(t, ok, "%s: index config should be readable", v.name)
				mv, ok := cfg["multivector"].(map[string]any)
				require.True(t, ok, "%s: multivector config should be present", v.name)
				require.Equal(t, true, mv["enabled"], "%s: multivector must be enabled", v.name)
				if v.muvera {
					muvera, ok := mv["muvera"].(map[string]any)
					require.True(t, ok, "%s: muvera config should be present", v.name)
					require.Equal(t, true, muvera["enabled"], "%s: muvera must be enabled", v.name)
				}
			}

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
		t.Run("every encoding owns directories on disk", func(t *testing.T) {
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
				}
			}
		})

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
	indexID := "vectors_" + targetVector
	names := map[string]struct{}{
		indexID:                              {}, // vectors bucket
		"vectors_compressed_" + targetVector: {}, // BQ/PQ/SQ compressed bucket
		indexID + "_muvera_vectors":          {}, // muvera's own bucket
		indexID + ".hnsw.commitlog.d":        {},
		indexID + ".hnsw.snapshot.d":         {},
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
		out := execInContainer(ctx, t, node.Container(),
			[]string{"find", "/", "-xdev", "-type", "d", "-name", "vectors_*"})
		for _, line := range strings.Split(out, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				dirs = append(dirs, node.Name()+":"+line)
			}
		}
	}
	return dirs
}

func hasSuffix(paths []string, suffix string) bool {
	for _, p := range paths {
		if strings.HasSuffix(p, suffix) {
			return true
		}
	}
	return false
}

func execInContainer(ctx context.Context, t *testing.T, c testcontainers.Container, cmd []string) string {
	t.Helper()
	code, reader, err := c.Exec(ctx, cmd, tcexec.Multiplexed())
	require.NoError(t, err, "exec %v", cmd)
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	require.NoError(t, err)
	// find exits non-zero only on a real error; "no matches" is exit 0 + empty.
	require.Zero(t, code, "exec %v failed: %s", cmd, buf.String())
	return buf.String()
}
