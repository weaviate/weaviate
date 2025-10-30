package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
)

func TestShardFileSanitize(t *testing.T) {
	// create a secret path that would be used in a malicious file path
	secretPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(secretPath, "secret.txt"), []byte("secret"), 0o600))

	ctx := testCtx()
	className := "TestClass"
	shd, _ := testShard(t, ctx, className)
	rootPath := shd.Index().Config.RootPath
	fmt.Println(rootPath)
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

	file, err := shd.GetFile(ctx, "../001/secret.txt")
	require.NoError(t, err)

	buf := make([]byte, 6)
	_, err = file.Read(buf)
	require.NoError(t, err)
}
