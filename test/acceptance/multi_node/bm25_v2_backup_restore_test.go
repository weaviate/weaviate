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

package multi_node

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// T7 - M3a: backup + restore of a V2-format BM25 cluster (filesystem backend,
// local/free; no S3/WCS).
//
// This proves the round-trip that the migration depends on: V2 flat-column
// property-length segments are backed up as opaque bytes and restored intact,
// and BM25 (including the >65535-token overflow doc) reads correctly off the
// RESTORED V2 segments - not just off the live pre-backup ones.
//
// See TestBm25V2CrossVersionRestoreReject (same file) for the M3b cross-version
// restore-reject: a V2 backup restored into a pre-guard binary must loud-fail.
func TestBm25V2BackupRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With1NodeCluster().
		WithBackendFilesystem().
		WithWeaviateEnv(bm25V2FlagEnv, "true").
		WithWeaviateEnv(bm25V2FlushReuse, "0").
		WithWeaviateEnv(bm25V2FlushDirty, "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	uri := compose.GetWeaviate().URI()
	helper.SetupClient(uri)

	class := bm25V2Class()
	class.ReplicationConfig = &models.ReplicationConfig{Factor: 1} // single node
	helper.CreateClass(t, class)
	for _, content := range bm25V2Corpus() {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      "DocV2",
			Properties: map[string]interface{}{"content": content},
		}))
	}

	// Ensure the data is flushed to a (V2) segment and queryable before backup.
	waitForBm25Ready(t, []string{uri})
	preScores := assertBm25CorrectFromNode(t, uri, "pre-backup")

	// Backup -> delete -> restore.
	const backupID = "bm25-v2-roundtrip"
	_, err = helper.CreateBackup(t, helper.DefaultBackupConfig(), "DocV2", "filesystem", backupID)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, e := helper.CreateBackupStatus(t, "filesystem", backupID, "", "")
		return e == nil && resp.Payload != nil && *resp.Payload.Status == "SUCCESS"
	}, 3*time.Minute, 2*time.Second, "backup did not reach SUCCESS")

	helper.DeleteClass(t, "DocV2")

	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), "DocV2", "filesystem", backupID, nil, false)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyRestored(t, backupID, "filesystem", nil, helper.WithDeadline(2*time.Minute))

	// Read back off the RESTORED segments: BM25 still correct, overflow lossless,
	// and scores byte-identical to pre-backup (a mis-restored length section
	// would shift the BM25 denominator and break this).
	waitForBm25Ready(t, []string{uri})
	postScores := assertBm25CorrectFromNode(t, uri, "post-restore")
	for content, want := range preScores {
		require.InDeltaf(t, want, postScores[content], 1e-9,
			"restored BM25 score for %q must equal pre-backup score", content[:min(20, len(content))])
	}
}

// defaultPreguardImage is the Docker image for the binary that predates the V2
// property-length guard. Override via TEST_WEAVIATE_PREGUARD_IMAGE.
const defaultPreguardImage = "weaviate-test:base-main-preguard"

func preguardImageName() string {
	if v := os.Getenv("TEST_WEAVIATE_PREGUARD_IMAGE"); v != "" {
		return v
	}
	return defaultPreguardImage
}

// TestBm25V2CrossVersionRestoreReject is M3b: the DANGER case.
//
// A filesystem backup taken from a V2-segment cluster, when restored into a
// pre-guard binary (one that predates the validateInvertedVersion guard and the
// V2 reader), must LOUD-FAIL rather than silently mis-decode and return wrong
// BM25 scores. This is the backup/restore analog of the rolling-upgrade
// forward-reject already verified for the live segment-open path.
//
// Mechanism:
//   - The V2 segment body encodes property lengths as a flat SoA column
//     (24B prefix + sorted uint64 docID column + parallel uint32 length column).
//   - The pre-guard binary's loadPropertyLengths calls gobenc.Decode on that body,
//     expecting the legacy gob-encoded map[uint64]uint32. The gob decoder sees a
//     mismatched preamble and returns a decode error.
//   - newSegment propagates the error (segment.go:410-412 in the pre-guard tree),
//     causing the bucket/shard to fail to open.
//   - On the restore path the segment open happens after the data files are copied
//     in, so the failure surfaces as a FAILED restore status - not as silent
//     mis-scoring.
//
// For the D2 binary (the new binary with the guard): restore copies files, then
// re-opens shards via the normal init path which calls newSegment ->
// LoadHeaderInverted -> validateInvertedVersion. This verifies the version guard
// fires on the RESTORED segment, not just on a normally-opened segment.
//
// Isolation: two successive single-node clusters, each on their own docker
// network (separate net octet). A host temp dir is bind-mounted at /tmp/backups
// in both containers so the backup written by the D2 node is visible to the
// pre-guard node. Teardown uses compose.Terminate which is scoped to the
// containers created here (never a global prune).
func TestBm25V2CrossVersionRestoreReject(t *testing.T) {
	d2Image := os.Getenv("TEST_WEAVIATE_IMAGE")
	if d2Image == "" {
		t.Skip("TEST_WEAVIATE_IMAGE not set; cross-version test requires pre-built image")
	}
	pgImage := preguardImageName()

	// Skip when the pre-guard image is not available locally. CI never builds
	// weaviate-test:base-main-preguard (it is a locally-assembled image); CI
	// nodes will get pull-access-denied and exit code 1, which would surface as
	// a test failure rather than a clean skip. The cross-version restore-reject
	// is verified by the d2_mixed_version manual rolling-upgrade harness and
	// local runs; CI does not need to re-prove it on every push.
	if err := exec.Command("docker", "image", "inspect", pgImage).Run(); err != nil {
		t.Skipf("cross-version restore-reject requires the locally-built pre-V2 image %q; "+
			"image not found locally (docker image inspect returned error: %v) - "+
			"this path is verified by the test/manual/d2_mixed_version rolling-upgrade harness and local runs",
			pgImage, err)
	}

	// Shared filesystem backup directory. OrbStack shares /tmp from the host
	// into Docker by default, so a temp dir under /tmp is visible to containers
	// via the bind mount below.
	backupDir, err := os.MkdirTemp("", "bm25-v2-xversion-backup-*")
	require.NoError(t, err)
	defer os.RemoveAll(backupDir)

	ctx := context.Background()

	// -----------------------------------------------------------------------
	// Phase 1: D2 binary writes V2 corpus and takes a filesystem backup.
	// -----------------------------------------------------------------------
	const backupID = "bm25-v2-xver-reject"

	composeD2, err := docker.New().
		With1NodeCluster().
		WithBackendFilesystem().
		WithWeaviateEnv(bm25V2FlagEnv, "true").
		WithWeaviateEnv(bm25V2FlushReuse, "0").
		WithWeaviateEnv(bm25V2FlushDirty, "1").
		WithWeaviateHostBind(backupDir, "/tmp/backups").
		Start(ctx)
	require.NoError(t, err, "D2 node failed to start")

	d2URI := composeD2.GetWeaviate().URI()
	helper.SetupClient(d2URI)

	classD2 := bm25V2Class()
	classD2.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	helper.CreateClass(t, classD2)
	for _, content := range bm25V2Corpus() {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      "DocV2",
			Properties: map[string]interface{}{"content": content},
		}))
	}

	// Wait until the overflow doc is indexed (V2 segment flushed and readable).
	waitForBm25Ready(t, []string{d2URI})

	_, err = helper.CreateBackup(t, helper.DefaultBackupConfig(), "DocV2", "filesystem", backupID)
	require.NoError(t, err, "create backup on D2 node")
	require.Eventually(t, func() bool {
		resp, e := helper.CreateBackupStatus(t, "filesystem", backupID, "", "")
		return e == nil && resp.Payload != nil && *resp.Payload.Status == "SUCCESS"
	}, 3*time.Minute, 2*time.Second, "D2 backup did not reach SUCCESS")

	// Terminate D2 before starting the pre-guard node so the two clusters
	// never share the same Docker network or IP space.
	require.NoError(t, composeD2.Terminate(ctx), "terminate D2 node")

	// -----------------------------------------------------------------------
	// Phase 2: pre-guard binary attempts to restore the V2 backup.
	// The pre-guard image must be started with TEST_WEAVIATE_IMAGE set to its
	// tag so the compose builder picks it up (it reads that env var to decide
	// which image to use instead of building from source).
	// -----------------------------------------------------------------------
	origImage := os.Getenv("TEST_WEAVIATE_IMAGE")
	os.Setenv("TEST_WEAVIATE_IMAGE", pgImage)
	composePG, pgStartErr := docker.New().
		With1NodeCluster().
		WithBackendFilesystem().
		WithWeaviateEnv(bm25V2FlushReuse, "0").
		WithWeaviateHostBind(backupDir, "/tmp/backups").
		Start(ctx)
	os.Setenv("TEST_WEAVIATE_IMAGE", origImage)

	if pgStartErr != nil {
		// A startup failure on an empty data dir is unexpected; fail the test
		// clearly so an operator can distinguish this from the expected restore
		// failure.
		t.Fatalf("pre-guard node failed to start on an empty data dir "+
			"(unexpected - the loud-fail should happen on restore, not startup): %v", pgStartErr)
	}
	defer func() { _ = composePG.Terminate(ctx) }()

	pgURI := composePG.GetWeaviate().URI()
	helper.SetupClient(pgURI)

	// -----------------------------------------------------------------------
	// Attempt restore. The restore API starts the operation asynchronously.
	// The pre-guard binary copies the segment files, then tries to open them.
	// The gobdec mismatch fires on shard init, causing the operation to fail.
	// We poll until FAILED or SUCCESS, then assert FAILED.
	// -----------------------------------------------------------------------
	_, restoreErr := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), "DocV2", "filesystem", backupID, nil, false)

	var restoreErrorText string

	if restoreErr != nil {
		// The restore API call itself returned an error (e.g. HTTP 4xx/5xx).
		// This is also a loud-fail: the pre-guard binary rejected the request.
		restoreErrorText = restoreErr.Error()
		t.Logf("CROSS-VERSION RESTORE-REJECT: restore API returned HTTP error: %v", restoreErr)
	} else {
		// Poll restore status until terminal (FAILED or SUCCESS) or timeout.
		deadline := time.Now().Add(3 * time.Minute)
		var finalStatus, finalError string
		for time.Now().Before(deadline) {
			resp, e := helper.RestoreBackupStatus(t, "filesystem", backupID, "", "")
			if e != nil || resp.Payload == nil {
				time.Sleep(2 * time.Second)
				continue
			}
			st := *resp.Payload.Status
			finalStatus = st
			if resp.Payload.Error != "" {
				finalError = resp.Payload.Error
			}
			if st == "FAILED" || st == "SUCCESS" {
				break
			}
			time.Sleep(2 * time.Second)
		}
		restoreErrorText = finalError

		// SUCCESS is the danger case: the pre-guard binary silently accepted the
		// V2 segment and is now serving wrong BM25 scores with no warning.
		assert.NotEqual(t, "SUCCESS", finalStatus,
			"pre-guard binary MUST NOT successfully restore a V2 backup; "+
				"SUCCESS means it silently accepted a V2 segment it cannot decode correctly "+
				"(wrong BM25 scores with no error - the failure mode the guard prevents)")

		require.Equal(t, "FAILED", finalStatus,
			"pre-guard binary must FAIL restoring a V2 backup (loud-fail required); "+
				"restore error: %s", finalError)

		t.Logf("CROSS-VERSION RESTORE-REJECT: restore status=FAILED (correct), error=%q", finalError)
	}

	// Capture container logs for the forensic record of the segment-open error
	// (gob decode mismatch, propagated from newSegment through the restore path).
	logLines := captureContainerLogs(ctx, t, composePG.GetWeaviate().Container())
	t.Logf("CROSS-VERSION RESTORE-REJECT: pre-guard container logs (last 80 lines):\n%s", logLines)

	// The error text must be non-empty. An empty error on a FAILED status could
	// mean a timeout or network drop - neither confirms the V2-segment decode
	// loud-fail we require.
	require.NotEmpty(t, restoreErrorText,
		"FAILED restore status must carry an error message describing the segment decode failure; "+
			"empty error string means the failure cause is unknown, not the V2-decode loud-fail we require")

	t.Logf("CROSS-VERSION RESTORE-REJECT: exact error text = %q", restoreErrorText)

	// Confirm the error is segment-decode-related and not an unrelated
	// infrastructure failure (network timeout, missing backup ID, etc.).
	// The gobdec path emits "preamble mismatch" / "gob: type mismatch"; the
	// shard init error wraps it with "bucket" / "shard" / "segment" context.
	lower := strings.ToLower(restoreErrorText)
	errorIsSegmentRelated := strings.Contains(lower, "segment") ||
		strings.Contains(lower, "preamble") ||
		strings.Contains(lower, "gob") ||
		strings.Contains(lower, "bucket") ||
		strings.Contains(lower, "shard") ||
		strings.Contains(lower, "decode") ||
		strings.Contains(lower, "version") ||
		strings.Contains(lower, "property")
	assert.True(t, errorIsSegmentRelated,
		"restore failure error must mention segment/bucket/shard/decode/gob/preamble/version/property "+
			"to confirm it is a V2-segment decode failure and not an unrelated infrastructure error; "+
			"actual error: %q", restoreErrorText)
}

// captureContainerLogs reads the last 80 log lines from a container.
// Returns an empty string on any error (best-effort forensic capture).
func captureContainerLogs(ctx context.Context, t *testing.T, c testcontainers.Container) string {
	t.Helper()
	if c == nil {
		return ""
	}
	reader, err := c.Logs(ctx)
	if err != nil {
		return ""
	}
	defer reader.Close()
	raw, _ := io.ReadAll(reader)
	lines := strings.Split(string(raw), "\n")
	if len(lines) > 80 {
		lines = lines[len(lines)-80:]
	}
	return strings.Join(lines, "\n")
}
