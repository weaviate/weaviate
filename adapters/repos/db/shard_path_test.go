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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
)

func TestShardFileSanitize(t *testing.T) {
	// create a secret path that would be used in a malicious file path
	secretPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(secretPath, "secret.txt"), []byte("secret"), 0o600))

	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)
	// 0 timeout disables inactivity auto-resume, so slow setup can't resume
	// the shard before ListBackupFiles runs.
	require.NoError(t, shd.HaltForTransfer(ctx, false, 0))
	amount := 10

	for range amount {
		obj := testObject(className)

		err := shd.PutObject(ctx, obj)
		require.Nil(t, err)
	}

	objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
	require.Nil(t, err)
	require.Equal(t, amount, len(objs))

	// try to read outside of the shard directory
	_, err = shd.GetFile(ctx, "../001/secret.txt")
	require.Error(t, err)
	_, err = shd.GetFileMetadata(ctx, "../001/secret.txt")
	require.Error(t, err)

	// create a second "fake" index and shard and try to read it
	otherShardDir := filepath.Join(idx.Config.RootPath, "otherIndex", "otherShard")
	require.NoError(t, os.MkdirAll(otherShardDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(otherShardDir, "secret.txt"), []byte("secret"), 0o700))

	file, err := shd.GetFile(ctx, filepath.Join(otherShardDir, "secret.txt"))
	require.Error(t, err)
	require.Nil(t, file)
	_, err = shd.GetFileMetadata(ctx, filepath.Join(otherShardDir, "secret.txt"))
	require.Error(t, err)

	// now read a valid file
	ret := &backup.ShardDescriptor{}
	_, err = shd.ListBackupFiles(ctx, ret)
	require.NoError(t, err)

	file, err = shd.GetFile(ctx, ret.ShardVersionPath)
	require.NoError(t, err)
	require.NotNil(t, file)
}

func TestShardFilePutterSanitize(t *testing.T) {
	ctx := testCtx()
	className := "TestClassFilePutter"
	shd, idx := testShard(t, ctx, className)

	// Reject path traversal that would write next to the data root.
	outsideName := "weaviate-outside-data-root.txt"
	wc, err := shd.filePutter(ctx, filepath.Join("..", outsideName))
	require.Error(t, err)
	require.Nil(t, wc)
	_, err = os.Stat(filepath.Join(filepath.Dir(idx.Config.RootPath), outsideName))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))

	// Reject absolute paths (e.g. /tmp/...).
	wc, err = shd.filePutter(ctx, filepath.Join(string(filepath.Separator), "tmp", "weaviate-fileputter-probe"))
	require.Error(t, err)
	require.Nil(t, wc)

	// Reject writes into another collection/shard under the same data root
	// (reproduction B from #13099).
	crossPath := filepath.Join("otherclass", "othershard", "injected.txt")
	wc, err = shd.filePutter(ctx, crossPath)
	require.Error(t, err)
	require.Nil(t, wc)
	_, err = os.Stat(filepath.Join(idx.Config.RootPath, crossPath))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))

	// A path under this shard (DB-relative) must still be writable.
	relUnderShard := filepath.Join(idx.ID(), shd.Name(), "safe-putter", "ok.txt")
	wc, err = shd.filePutter(ctx, relUnderShard)
	require.NoError(t, err)
	require.NotNil(t, wc)
	_, err = wc.Write([]byte("ok"))
	require.NoError(t, err)
	require.NoError(t, wc.Close())
	content, err := os.ReadFile(filepath.Join(idx.Config.RootPath, relUnderShard))
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), content)
}
