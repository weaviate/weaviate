//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	numClasses = 100
	numShards  = 10
	uppercase  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	lowercase  = "abcdefghijklmnopqrstuvwxyz"
	digits     = "0123456789"
	chars      = uppercase + lowercase + digits
	localNode  = "node1"
)

var (
	rootFiles = []string{
		"classifications.db",
		"modules.db",
		"schema.db",
	}
	indexDirExts = []string{
		".hnsw.commitlog.d",
		"_someGeoProp.hnsw.commitlog.d",
		"_lsm",
	}
	indexFileExts = []string{
		".indexcount",
		".proplengths",
		".version",
	}
	migratedRootFiles = append(rootFiles,
		"migration1.22.fs.hierarchy")
)

func TestFileStructureMigration(t *testing.T) {
	shardsByClass := make(map[string][]string, numClasses)

	t.Run("generate index and shard names", func(t *testing.T) {
		for i := 0; i < numClasses; i++ {
			c := randClassName()
			shardsByClass[c] = make([]string, numShards)
			for j := 0; j < numShards; j++ {
				s := randShardName()
				shardsByClass[c][j] = s
			}
		}
	})

	root := t.TempDir()

	t.Run("write test db files", func(t *testing.T) {
		for _, f := range rootFiles {
			require.Nil(t, os.WriteFile(path.Join(root, f), nil, os.ModePerm))
		}

		for class, shards := range shardsByClass {
			for _, shard := range shards {
				idx := path.Join(root, fmt.Sprintf("%s_%s", strings.ToLower(class), shard))
				for _, ext := range indexDirExts {
					require.Nil(t, os.MkdirAll(idx+ext, os.ModePerm))
				}
				for _, ext := range indexFileExts {
					require.Nil(t, os.WriteFile(idx+ext, nil, os.ModePerm))
				}

				pqDir := path.Join(root, class, shard, "compressed_objects")
				require.Nil(t, os.MkdirAll(pqDir, os.ModePerm))
			}
		}
	})

	files, err := os.ReadDir(root)
	require.Nil(t, err)

	t.Run("assert expected flat contents length", func(t *testing.T) {
		// Flat structure root contains:
		//  - (3 dirs + 3 files) per shard per index
		//    - dirs: main commilog, geo prop commitlog, lsm store
		//    - files: indexcount, proplengths, version
		//  - 1 dir per index; shards dirs are nested
		//    - pq store
		//  - 3 root db files
		expectedLen := numClasses*(numShards*(len(indexDirExts)+len(indexFileExts))+1) + len(rootFiles)
		require.Len(t, files, expectedLen)
	})

	t.Run("migrate the db", func(t *testing.T) {
		classes := make([]*models.Class, numClasses)
		states := make(map[string]*sharding.State, numClasses)

		i := 0
		for class, shards := range shardsByClass {
			classes[i] = &models.Class{
				Class: class,
				Properties: []*models.Property{{
					Name:     "someGeoProp",
					DataType: schema.DataTypeGeoCoordinates.PropString(),
				}},
			}
			states[class] = &sharding.State{
				Physical: make(map[string]sharding.Physical),
			}
			states[class].SetLocalName(localNode)

			for _, shard := range shards {
				states[class].Physical[shard] = sharding.Physical{
					Name:           shard,
					BelongsToNodes: []string{localNode},
				}
			}

			i++
		}

		db := testDB(root, classes, states)
		require.Nil(t, db.migrateFileStructureIfNecessary())
	})

	files, err = os.ReadDir(root)
	require.Nil(t, err)

	t.Run("assert expected hierarchical contents length", func(t *testing.T) {
		// After migration, the hierarchical structure root contains:
		//  - one dir per index
		//  - 3 original root db files, and one additional which is the FS migration indicator
		expectedLen := numClasses + len(migratedRootFiles)
		require.Len(t, files, expectedLen)
	})

	t.Run("assert all db files were migrated", func(t *testing.T) {
		var foundRootFiles []string
		for _, f := range files {
			if f.IsDir() {
				idx := f
				shardsRoot, err := os.ReadDir(path.Join(root, idx.Name()))
				require.Nil(t, err)
				for _, shard := range shardsRoot {
					assertShardRootContents(t, shardsByClass, root, idx, shard)
				}
			} else {
				foundRootFiles = append(foundRootFiles, f.Name())
			}
		}

		assert.ElementsMatch(t, migratedRootFiles, foundRootFiles)
	})
}

func assertShardRootContents(t *testing.T, shardsByClass map[string][]string, root string, idx, shard os.DirEntry) {
	assert.True(t, shard.IsDir())

	// Whatever we find in this shard directory, it should be able to
	// be mapped back to the original flat structure root contents
	lowercasedClasses := make(map[string]string, len(shardsByClass))
	for class := range shardsByClass {
		lowercasedClasses[strings.ToLower(class)] = class
	}
	require.Contains(t, lowercasedClasses, idx.Name())
	assert.Contains(t, shardsByClass[lowercasedClasses[idx.Name()]], shard.Name())

	// Now we will get a set of all expected files within the shard dir.
	// Check to see if all of these files are found.
	expected := expectedShardContents()
	shardFiles, err := os.ReadDir(path.Join(root, idx.Name(), shard.Name()))
	require.Nil(t, err)
	for _, sf := range shardFiles {
		expected[sf.Name()] = true
	}
	expected.assert(t)

	// Check if pq store was migrated to main store as "vectors_compressed" subdir
	pqDir := path.Join(root, idx.Name(), shard.Name(), "lsm", helpers.VectorsCompressedBucketLSM)
	info, err := os.Stat(pqDir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func testDB(root string, classes []*models.Class, states map[string]*sharding.State) *DB {
	logger, _ := test.NewNullLogger()
	return &DB{
		config: Config{RootPath: root},
		logger: logger,
		schemaGetter: &fakeMigrationSchemaGetter{
			sch:    schema.Schema{Objects: &models.Schema{Classes: classes}},
			states: states,
		},
	}
}

func randClassName() string {
	return randStringBytes(16)
}

func randShardName() string {
	return randStringBytes(8)
}

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		switch {
		case i == 0:
			b[i] = randChar(uppercase)
		case i == n/2:
			b[i] = []byte("_")[0]
		default:
			b[i] = randChar(chars)
		}
	}
	return string(b)
}

func randChar(str string) byte {
	return str[rand.Intn(len(str))]
}

type shardContents map[string]bool

func expectedShardContents() shardContents {
	return shardContents{
		"main.hnsw.commitlog.d":            false,
		"geo.someGeoProp.hnsw.commitlog.d": false,
		"lsm":                              false,
		"indexcount":                       false,
		"proplengths":                      false,
		"version":                          false,
	}
}

func (c shardContents) assert(t *testing.T) {
	for name, found := range c {
		assert.True(t, found, "didn't find %q in shard contents", name)
	}
}

type fakeMigrationSchemaGetter struct {
	sch    schema.Schema
	states map[string]*sharding.State
}

func (sg *fakeMigrationSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return sg.sch
}

func (sg *fakeMigrationSchemaGetter) Nodes() []string {
	return nil
}

func (sg *fakeMigrationSchemaGetter) NodeName() string {
	return ""
}

func (sg *fakeMigrationSchemaGetter) ClusterHealthScore() int {
	return 0
}

func (sg *fakeMigrationSchemaGetter) ResolveParentNodes(string, string) (map[string]string, error) {
	return nil, nil
}

func (sg *fakeMigrationSchemaGetter) CopyShardingState(class string) *sharding.State {
	return sg.states[class]
}

func (sg *fakeMigrationSchemaGetter) ShardOwner(class, shard string) (string, error) {
	return "", nil
}

func (sg *fakeMigrationSchemaGetter) TenantShard(class, tenant string) (string, string) {
	return "", ""
}

func (sg *fakeMigrationSchemaGetter) ShardFromUUID(class string, uuid []byte) string {
	return ""
}

func (sg *fakeMigrationSchemaGetter) ShardReplicas(class, shard string) ([]string, error) {
	return nil, nil
}
