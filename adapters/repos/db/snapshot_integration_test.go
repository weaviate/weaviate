//go:build integrationTest
// +build integrationTest

package db

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_BucketLevel(t *testing.T) {
	ctx := testCtx()
	className := "BucketLevelSnapshot"
	shard, _ := testShard(ctx, className)

	t.Run("insert data", func(t *testing.T) {
		err := shard.putObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    "8c29da7a-600a-43dc-85fb-83ab2b08c294",
				Class: className,
				Properties: map[string]interface{}{
					"stringField": "somevalue",
				},
			}},
		)
		require.Nil(t, err)
	})

	t.Run("perform snapshot sequence", func(t *testing.T) {
		objBucket := shard.store.Bucket("objects")
		require.NotNil(t, objBucket)

		err := objBucket.PauseCompaction(ctx)
		require.Nil(t, err)

		err = objBucket.FlushMemtable(ctx)
		require.Nil(t, err)

		files, err := objBucket.ListFiles(ctx)
		require.Nil(t, err)

		t.Run("check ListFiles, results", func(t *testing.T) {
			assert.Len(t, files, 2)

			// build regex to get very close approximation to the expected
			// contents of the ListFiles result. the only thing we can't
			// know for sure is the actual name of the segment group, hence
			// the `.*`
			parent, child := path.Split(shard.index.Config.RootPath)
			re := fmt.Sprintf("%s\\/%s", path.Clean(parent), child)
			re = fmt.Sprintf("%s\\/bucketlevelsnapshot_%s_lsm\\/objects\\/.*\\.(wal|db)", re, shard.name)
			re = path.Clean(re)

			// we expect to see only two files inside the bucket at this point:
			//   1. a *.db file
			//   2. a *.wal file
			//
			// both of these files are the result of the above FlushMemtable's
			// underlying call to FlushAndSwitch. The *.db is the flushed original
			// WAL, and the *.wal is the new one
			isMatch, err := regexp.MatchString(re, files[0])
			assert.Nil(t, err)
			assert.True(t, isMatch)
			isMatch, err = regexp.MatchString(re, files[1])
			assert.True(t, isMatch)

			// check that we have one of each: *.db, *.wal
			if strings.HasSuffix(files[0], ".db") {
				assert.True(t, strings.HasSuffix(files[0], ".db"))
				assert.True(t, strings.HasSuffix(files[1], ".wal"))
			} else {
				assert.True(t, strings.HasSuffix(files[0], ".wal"))
				assert.True(t, strings.HasSuffix(files[1], ".db"))
			}
		})

		err = objBucket.ResumeCompaction(ctx)
		require.Nil(t, err)
	})

	t.Run("shutdown shard", func(t *testing.T) {
		require.Nil(t, shard.shutdown(ctx))
	})
}
