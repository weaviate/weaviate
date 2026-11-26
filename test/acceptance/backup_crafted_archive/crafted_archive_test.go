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

// Package backup_crafted_archive covers restoring backups whose chunks carry entries outside the class index dir.
package backup_crafted_archive

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

const (
	backend     = "filesystem"
	backupsRoot = "/tmp/backups"
	dataRoot    = "/data"
	className   = "Movies"
	numObjects  = 5
	evilContent = "evil"
)

// TestRestoreRefusesCraftedTopLevelEntries asserts a chunk with top-level entries besides the class dir fails the restore untouched.
func TestRestoreRefusesCraftedTopLevelEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().WithBackendFilesystem().WithWeaviate().Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
		},
	})
	objects := make([]*models.Object, numObjects)
	for i := range objects {
		objects[i] = &models.Object{
			Class:      className,
			ID:         strfmt.UUID(uuid.NewString()),
			Properties: map[string]any{"title": fmt.Sprintf("movie %d", i)},
			Vector:     []float32{float32(i), 1, 2},
		}
	}
	helper.CreateObjectsBatch(t, objects)
	require.EqualValues(t, numObjects, moduleshelper.GetClassCount(t, className, ""))

	cfg := helper.DefaultBackupConfig()
	cfg.CompressionLevel = models.BackupConfigCompressionLevelNoCompression
	const craftedID, cleanID = "crafted-archive", "clean-archive"
	for _, id := range []string{craftedID, cleanID} {
		_, err := helper.CreateBackup(t, cfg, className, backend, id)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, id, backend, nil)
	}

	container := compose.GetWeaviate().Container()
	require.True(t, pathExists(ctx, t, container, dataRoot+"/"+strings.ToLower(className)))
	_, hadClassifications := readFile(ctx, t, container, dataRoot+"/classifications.db")

	craftChunk(ctx, t, container, craftedID)

	helper.DeleteClass(t, className)

	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backend, craftedID, nil, false)
	require.NoError(t, err)
	status, errMsg := awaitRestoreTerminal(t, craftedID)
	t.Logf("crafted restore: status=%s error=%q", status, errMsg)
	assert.Equal(t, "FAILED", status, "restore of a crafted archive must fail, error: %q", errMsg)
	assert.Contains(t, errMsg, "foreign entries")

	after, exists := readFile(ctx, t, container, dataRoot+"/classifications.db")
	t.Logf("classifications.db existed before: %v, exists after: %v, holds %q: %v", hadClassifications, exists, evilContent, strings.Contains(after, evilContent))
	assert.NotContains(t, after, evilContent, "classifications.db was replaced from the archive")
	assert.Equal(t, hadClassifications, exists)
	assert.False(t, pathExists(ctx, t, container, dataRoot+"/otherclass"), "otherclass was planted in the data root")
	assert.False(t, pathExists(ctx, t, container, dataRoot+"/.backup.tmp/"+className), "staging dir was left behind")

	_, err = helper.GetClassWithoutAssert(t, className, "")
	var notFound *schema.SchemaObjectsGetNotFound
	assert.ErrorAs(t, err, &notFound, "failed restore must not create the class")
	require.False(t, t.Failed(), "crafted archive was not refused")

	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())
	container = compose.GetWeaviate().Container()
	requireNodeHealthy(t)
	after, _ = readFile(ctx, t, container, dataRoot+"/classifications.db")
	require.NotContains(t, after, evilContent)

	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backend, cleanID, nil, false)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyRestored(t, cleanID, backend, nil)
	require.EqualValues(t, numObjects, moduleshelper.GetClassCount(t, className, ""))
}

func craftChunk(ctx context.Context, t *testing.T, c testcontainers.Container, backupID string) {
	t.Helper()
	code, out, err := c.Exec(ctx, []string{"find", backupsRoot + "/" + backupID, "-type", "f", "-name", "chunk-*"}, tcexec.Multiplexed())
	require.NoError(t, err)
	require.Zero(t, code)
	raw, err := io.ReadAll(out)
	require.NoError(t, err)
	var chunks []string
	for _, p := range strings.Fields(string(raw)) {
		if strings.Contains(p, "/"+className+"/") {
			chunks = append(chunks, p)
		}
	}
	require.NotEmpty(t, chunks, "no chunk for %s in %s", className, raw)
	sort.Strings(chunks)
	chunkPath := chunks[0]

	rc, err := c.CopyFileFromContainer(ctx, chunkPath)
	require.NoError(t, err)
	original, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tr := tar.NewReader(bytes.NewReader(original))
	kept := 0
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(hdr.Name, strings.ToLower(className)+"/"), "unexpected entry %q", hdr.Name)
		require.NoError(t, tw.WriteHeader(hdr))
		_, err = io.Copy(tw, tr)
		require.NoError(t, err)
		kept++
	}
	require.Positive(t, kept)
	writeTarFile(t, tw, "classifications.db", evilContent)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "otherclass/", Typeflag: tar.TypeDir, Mode: 0o755, ModTime: time.Now()}))
	writeTarFile(t, tw, "otherclass/x", evilContent)
	require.NoError(t, tw.Close())

	require.NoError(t, c.CopyToContainer(ctx, buf.Bytes(), chunkPath, 0o644))
}

func writeTarFile(t *testing.T, tw *tar.Writer, name, content string) {
	t.Helper()
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content)), ModTime: time.Now(),
	}))
	_, err := tw.Write([]byte(content))
	require.NoError(t, err)
}

func awaitRestoreTerminal(t *testing.T, backupID string) (status, errMsg string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.RestoreBackupStatus(t, backend, backupID, "", "")
		require.NoError(c, err)
		require.NotNil(c, resp.Payload)
		require.NotNil(c, resp.Payload.Status)
		status, errMsg = *resp.Payload.Status, resp.Payload.Error
		require.Contains(c, []string{"SUCCESS", "FAILED", "CANCELED"}, status)
	}, 60*time.Second, 500*time.Millisecond)
	return status, errMsg
}

func requireNodeHealthy(t *testing.T) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Nodes.NodesGet(nil, nil)
		require.NoError(c, err)
		require.Len(c, resp.Payload.Nodes, 1)
		require.Equal(c, models.NodeStatusStatusHEALTHY, *resp.Payload.Nodes[0].Status)
	}, 60*time.Second, 500*time.Millisecond)
}

func pathExists(ctx context.Context, t *testing.T, c testcontainers.Container, p string) bool {
	t.Helper()
	code, _, err := c.Exec(ctx, []string{"test", "-e", p})
	require.NoError(t, err)
	return code == 0
}

func readFile(ctx context.Context, t *testing.T, c testcontainers.Container, p string) (string, bool) {
	t.Helper()
	if !pathExists(ctx, t, c, p) {
		return "", false
	}
	code, out, err := c.Exec(ctx, []string{"cat", p}, tcexec.Multiplexed())
	require.NoError(t, err)
	require.Zero(t, code)
	b, err := io.ReadAll(out)
	require.NoError(t, err)
	return string(b), true
}
