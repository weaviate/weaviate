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

	objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName, nil, nil)
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
	require.NoError(t, shd.ListBackupFiles(ctx, ret))

	file, err = shd.GetFile(ctx, ret.ShardVersionPath)
	require.NoError(t, err)
	require.NotNil(t, file)
}
