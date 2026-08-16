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

package reindex_backup_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	clientbackups "github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	holdWindowTenants = 150
	// Only has to keep the migration alive until the tracker count below is reached.
	holdWindowObjectsPerTenant = 40
	// Trackers, not tenants: a shard with nothing to sweep is skipped without being loaded.
	holdWindowTrackerShards = holdWindowTenants * 2 / 3
)

// The unit tests pin what the hold arm answers; this pins that a real cleanup raises one
// a real restore then sees, across the REST hop and a live migration's teardown.
//
// The probe must start before the cancel; the window it samples can close before the task reaches a terminal status.
func TestRestoreRefusedByCleanupHold(t *testing.T) {
	ctx := context.Background()
	compose := startGuardNode(ctx, t)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })
	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	const (
		className = "RestoreHold_Migrating"
		propName  = "body"
		backend   = "filesystem"
		// Never created: only an unknown id is side-effect free enough to retry at speed.
		unknownBackupID = "restore-hold-probe-no-such-backup"
	)

	createHoldWindowClass(t, className, propName)
	t.Cleanup(func() { helper.DeleteClassWithTimeout(t, className, 5*time.Minute) })

	taskID := submitChangeTokenization(t, restURI, className, propName, "lowercase")
	t.Logf("hold-window task submitted: %s", taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(120*time.Second))
	awaitTrackerDirs(t, ctx, compose.GetWeaviate().Container(), className, propName,
		holdWindowTrackerShards, 120*time.Second)
	probe := startHoldProbe(t, backend, unknownBackupID, className)
	reindexhelpers.CancelIndexRaw(t, restURI, className, propName, "searchable")

	refusal := probe.awaitHoldRefusal(t, 120*time.Second)
	require.NotEmptyf(t, refusal,
		"the teardown window closed before any probe landed; raise holdWindowTrackerShards "+
			"and holdWindowObjectsPerTenant so the cleanup has more to delete")

	require.Containsf(t, refusal, className,
		"the refusal must name the collection it is about; got: %s", refusal)
	requireNoPlacement(t, refusal, "")

	require.Eventuallyf(t, func() bool {
		var missing *clientbackups.BackupsRestoreNotFound
		return errors.As(restoreClasses(t, backend, unknownBackupID, className), &missing)
	}, 120*time.Second, 250*time.Millisecond,
		"once the cleanup drains the same probe must fall through to 404")
}

func createHoldWindowClass(t *testing.T, className, propName string) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Vectorizer:         "none",
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
	})
	tenants := make([]*models.Tenant, 0, holdWindowTenants)
	for i := range holdWindowTenants {
		tenants = append(tenants, &models.Tenant{
			Name:           fmt.Sprintf("tenant-%03d", i),
			ActivityStatus: models.TenantActivityStatusHOT,
		})
	}
	helper.CreateTenants(t, className, tenants)
	for _, tenant := range tenants {
		importTenantBodies(t, className, propName, tenant.Name, holdWindowObjectsPerTenant)
	}
}

func importTenantBodies(t *testing.T, className, propName, tenant string, count int) {
	t.Helper()
	body := "Alpha Bravo Charlie Delta Echo Foxtrot Golf Hotel India Juliett"
	objects := make([]*models.Object, count)
	for i := range objects {
		objects[i] = &models.Object{
			Class:      className,
			ID:         strfmt.UUID(uuid.New().String()),
			Tenant:     tenant,
			Properties: map[string]interface{}{propName: body},
		}
	}
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{Objects: objects})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// Both gate arms answer 422, so only this wording attributes a refusal to the cleanup hold.
const holdRefusalMarker = "still removing its temporary index files"

type holdProbe struct {
	mu      sync.Mutex
	refusal string
	liveArm int
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func startHoldProbe(t *testing.T, backend, backupID, className string) *holdProbe {
	t.Helper()
	p := &holdProbe{stop: make(chan struct{}), done: make(chan struct{})}
	t.Cleanup(p.shutdown)
	go func() {
		defer close(p.done)
		for {
			select {
			case <-p.stop:
				return
			default:
			}
			var refused *clientbackups.BackupsRestoreUnprocessableEntity
			if errors.As(restoreClasses(t, backend, backupID, className), &refused) {
				p.record(errorResponseMessage(refused.Payload))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return p
}

func (p *holdProbe) record(msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !strings.Contains(msg, holdRefusalMarker) {
		p.liveArm++
		return
	}
	if p.refusal == "" {
		p.refusal = msg
	}
}

func (p *holdProbe) awaitHoldRefusal(t *testing.T, timeout time.Duration) string {
	t.Helper()
	defer p.shutdown()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		refusal, liveArm := p.refusal, p.liveArm
		p.mu.Unlock()
		if refusal != "" {
			t.Logf("the hold answered a probe; the live-task arm had answered %d before it", liveArm)
			return refusal
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func (p *holdProbe) shutdown() {
	p.once.Do(func() {
		close(p.stop)
		<-p.done
	})
}

// Without this wait the cleanup has no tracker to delete and the hold never opens.
func awaitTrackerDirs(t *testing.T, ctx context.Context, container testcontainers.Container,
	className, propName string, want int, timeout time.Duration,
) {
	t.Helper()
	script := fmt.Sprintf(`ls -d /data/%s/*/lsm/.migrations/*_%s* 2>/dev/null | wc -l`,
		strings.ToLower(className), propName)
	deadline := time.Now().Add(timeout)
	last := 0
	for time.Now().Before(deadline) {
		code, reader, err := container.Exec(ctx, []string{"sh", "-c", script})
		require.NoError(t, err)
		require.Zero(t, code)
		out := new(strings.Builder)
		if reader != nil {
			_, _ = io.Copy(out, reader)
		}
		last, _ = strconv.Atoi(strings.TrimSpace(stripExecFrames(out.String())))
		if last >= want {
			t.Logf("%d shards carry a tracker for %q", last, propName)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	if last == 0 {
		t.Fatalf("no shard carried a tracker for %q within %s, so the cleanup would have "+
			"nothing to sweep and this test could observe no hold at all", propName, timeout)
	}
	t.Fatalf("only %d of %d shards carried a tracker for %q within %s; raise "+
		"holdWindowObjectsPerTenant so the migration outlives this poll", last, want, propName, timeout)
}

// stripExecFrames drops the header bytes the docker exec API prefixes each chunk with.
func stripExecFrames(raw string) string {
	var out strings.Builder
	for _, r := range raw {
		if r >= ' ' || r == '\n' {
			out.WriteRune(r)
		}
	}
	return out.String()
}
