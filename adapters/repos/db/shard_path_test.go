package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
	require.NoError(t, shd.HaltForTransfer(ctx, false, 100*time.Millisecond))
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

	// create a second "fake" index and shard and try to read it
	otherShardDir := filepath.Join(idx.Config.RootPath, "otherIndex", "otherShard")
	require.NoError(t, os.MkdirAll(otherShardDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(otherShardDir, "secret.txt"), []byte("secret"), 0o700))

	file, err := shd.GetFile(ctx, filepath.Join(otherShardDir, "secret.txt"))
	require.Error(t, err)
	require.Nil(t, file)

	// now read a valid file
	ret := &backup.ShardDescriptor{}
	require.NoError(t, shd.ListBackupFiles(ctx, ret))

	file, err = shd.GetFile(ctx, ret.ShardVersionPath)
	require.NoError(t, err)
	require.NotNil(t, file)
}
