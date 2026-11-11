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

package tachiomfresh

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// A vector dropped while its documents are staged, then created again
// under the same name, starts empty. The drop is the shard's: the index's
// Drop, then every bucket and directory of the target's artifact set. A
// bucket the set misses is opened again by the new index, which then
// answers with the old documents and builds on them.
func TestDropThenRecreateStartsEmpty(t *testing.T) {
	const target = "vec"
	const tokens, dims = 4, 8
	tests := []struct {
		name string
		docs int
	}{
		{name: "one staged document", docs: 1},
		{name: "several staged documents", docs: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			corpus, random := centeredCorpus(tt.docs, tokens, dims, 11)
			dir := t.TempDir()
			storeDir := filepath.Join(dir, "store")
			open := func() testIndex {
				tf := newTestIndexAt(t, dir, 8, 8, 8, hfreshEFConstruction,
					withDistanceProvider(distancer.NewDotProductProvider()),
					withID(helpers.GetVectorsBucketName(target)))
				// above the corpus: every document stays staged
				tf.Index.setCenterTokens(uint64(100 * tokens))
				return tf
			}

			tf := open()
			for d := range tt.docs {
				tf.mvStore.storeMultiVector(uint64(d), corpus[d])
				require.NoError(t, tf.Index.AddMulti(t.Context(), uint64(d), corpus[d]))
			}
			require.False(t, tf.Index.built.Load())
			staging := filepath.Join(storeDir, "tachiomfresh_staging_vectors_vec")
			require.DirExists(t, staging, "precondition: the staged documents are in their bucket")

			require.NoError(t, tf.Index.Drop(t.Context(), false))
			artifacts := helpers.VectorIndexArtifactsFor(target, nil)
			for _, name := range artifacts.LSMBuckets {
				if tf.store.Bucket(name) != nil {
					require.NoError(t, tf.store.ShutdownBucket(t.Context(), name))
				}
				require.NoError(t, os.RemoveAll(filepath.Join(storeDir, name)))
			}
			// the shard removes the index's own directory with the other
			// shard directories; here the index's root plays that part
			require.NoError(t, os.RemoveAll(filepath.Join(dir, "root")))
			require.NoError(t, tf.store.Shutdown(t.Context()))
			require.NoDirExists(t, staging, "the drop must remove the staging bucket")

			tf = open()
			defer tf.close(t)
			require.Zero(t, tf.Index.stagedTokens)
			for d := range tt.docs {
				require.False(t, tf.Index.ContainsDoc(uint64(d)), "doc %d", d)
			}
			ids, _, err := tf.Index.SearchByMultiVector(t.Context(), [][]float32{random(), random()}, 5, nil)
			require.NoError(t, err)
			require.Empty(t, ids)
		})
	}
}
