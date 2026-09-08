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

package namespace

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// How many log lines per node requireNoApplyFailure reads back. Generous enough
// to span cluster boot plus the serial tests ahead of this one.
const applyLogTail = 4000

// requireNoApplyFailure fails if any node logged a failed apply of applyType for
// this class. Scoped by both so neither an unrelated class elsewhere in the shared
// cluster nor the freeze's own doomed upload reads as the failure under test.
func requireNoApplyFailure(t *testing.T, applyType, qualifiedClass string) {
	t.Helper()

	var logs bytes.Buffer
	sharedCompose.DumpWeaviateLogs(context.Background(), &logs, applyLogTail)
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "apply command") &&
			strings.Contains(line, applyType) &&
			strings.Contains(line, qualifiedClass) {
			t.Fatalf("a %s apply failed for %q: %s", applyType, qualifiedClass, line)
		}
	}
}

// An offload that finishes after its namespace was suspended still has to report
// what it did. The report is the one command whose schema half commits while its
// DB half meets a namespace holding its shards closed, so refusing it there would
// leave the tenant FREEZING with nothing left to move it: no retry re-sends the
// report, and the schema allows no other status change out of FREEZING.
//
// With MinIO down the upload fails on OFFLOAD_TIMEOUT rather than at once, and
// the freeze apply holds the apply loop for that whole time. The suspend below
// therefore reaches the RAFT log well ahead of the report the abort sends after
// it, which is what puts the two in the order this test needs.
//
// Serial on purpose: MinIO also backs backup-s3 and export, so no parallel test
// may run while it is down.
func TestNamespaces_SuspendDuringOffloadAbort(t *testing.T) {
	ctx := context.Background()
	ns1, _, user1Key, _ := twoNamespaces(t)

	const (
		class  = "SuspendOffload"
		tenant = "offloaded"
	)
	setupMTClassInNs1(t, ns1, class, user1Key)
	qualified := ns1 + ":" + class

	require.NoError(t, addTenantsAuth(t, qualified, []*models.Tenant{
		{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT},
	}, adminKey))
	requireShardsEventually(t, qualified, tenant)

	// Uploading an empty shard directory is a no-op that reports success, so the
	// tenant needs an object for the freeze to reach MinIO at all.
	_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
		Class:      class,
		Tenant:     tenant,
		Properties: map[string]any{"title": "written before the freeze"},
	}, user1Key)
	require.NoError(t, err)

	// Paused rather than stopped: MinIO is shared with backup-s3 and export, and a
	// stop would delete it for the rest of the package.
	require.NoError(t, sharedCompose.PauseMinIO(ctx))
	t.Cleanup(func() { require.NoError(t, sharedCompose.UnpauseMinIO(ctx)) })

	// The freeze goes in while the namespace is still active, since a suspended one
	// refuses the status change outright. Its call blocks for as long as the doomed
	// upload runs, so it cannot be what issues the suspend.
	freezeReturned := make(chan error, 1)
	go func() {
		freezeReturned <- updateTenantsAuth(t, qualified, []*models.Tenant{
			{Name: tenant, ActivityStatus: models.TenantActivityStatusFROZEN},
		}, adminKey)
	}()

	// The freeze has to reach the log before the suspend does. Suspending first
	// would refuse the freeze outright, leaving the tenant HOT and nothing for the
	// assertions below to catch. FREEZING is the freeze's schema half having
	// committed, so the suspend that follows it can only be a later log entry.
	requireTenantEventually(t, qualified, tenant, models.TenantActivityStatusFREEZING)

	helper.SuspendNamespace(t, ns1, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, ns1, adminKey) })

	// Reverting to HOT is the abort report being applied. Gating that report rather
	// than admitting its shard load would strand the tenant in FREEZING instead.
	waitForTenantStatus(t, qualified, tenant, models.TenantActivityStatusHOT, adminKey)

	// The status alone cannot tell an admitted shard load from a failed one, because
	// the schema half commits either way. The apply error is where they differ.
	requireNoApplyFailure(t, "TYPE_TENANT_PROCESS", qualified)

	// Waiting on the freeze call says its apply finished rather than still holding
	// the apply loop. How the upload this test broke surfaces to the caller is the
	// freeze's own contract, so the value is not asserted here.
	<-freezeReturned

	// A tenant the report put back to HOT has to be usable again once the namespace
	// is back, off the files the abort left behind.
	//
	// Retried: the resume is confirmed by reading the namespace back, which is
	// answered by the leader, while a namespaced key is authenticated against the
	// serving node's own copy of the state. A follower that has not applied the
	// resume yet still rejects the key with a 401, so the first write after a
	// resume can fail on a namespace the API already reports as active. The id is
	// left to the server, so a retry writes another object rather than colliding.
	t.Run("the tenant takes writes again after the resume", func(t *testing.T) {
		helper.ResumeNamespace(t, ns1, adminKey)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
				Class:      class,
				Tenant:     tenant,
				Properties: map[string]any{"title": "written after the resume"},
			}, user1Key)
			assert.NoError(c, err)
		}, 30*time.Second, 250*time.Millisecond, "the tenant never took a write after the resume")
	})
}
