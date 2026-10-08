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

package selfrecovery

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	srCrashBasePrefix = "00000000-0000-0000-0000"
	srCrashGapPrefix  = "55555555-5555-5555-5555"
)

// srChangelogPresent reports whether node idx holds a change-capture log for shard.
func srChangelogPresent(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int, shard string) bool {
	t.Helper()
	c, err := compose.ContainerAt(idx)
	if err != nil {
		return false
	}
	code, reader, err := c.Container().Exec(ctx, []string{"find", "/data", "-path", "*/" + shard + "/changelog/*.log"})
	if err != nil || code != 0 {
		return false
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "/changelog/")
}

// srCrashRig is a 3-node cluster whose target node was wiped after a baseline ingest, with its SELF_RECOVERY op located.
type srCrashRig struct {
	compose    *docker.DockerCompose
	class      string
	target     string
	op         srOp
	donorIdx   int
	controlIdx int
	controlURI string
}

const (
	srCrashWipedIdx = 2
	srCrashTarget   = docker.Weaviate2
)

// startSrCrashRig boots a persistent-data cluster with env, ingests baseCount objects into a single-shard class and wipes the target.
func startSrCrashRig(ctx context.Context, t *testing.T, class string, baseCount int, env map[string]string) *srCrashRig {
	t.Helper()
	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, persistentData: true, env: env})
	dumpLogsOnFailure(t, compose)

	mustRun(t, "wait for cluster to form quorum", func(t *testing.T) { waitClusterHealthy(t) })
	mustRun(t, "create collection", func(t *testing.T) {
		ensureClass(t, srParagraphClass(class))
		waitShardsLoaded(t, class, 1)
	})
	mustRun(t, "ingest baseline objects", func(t *testing.T) {
		submitBatch(t, srParagraphObjects(class, srCrashBasePrefix, baseCount, ""), types.ConsistencyLevelAll)
	})
	mustRun(t, "wipe and restart the target", func(t *testing.T) { wipeAndRestart(ctx, t, compose, srCrashWipedIdx) })

	r := &srCrashRig{compose: compose, class: class, target: srCrashTarget, op: findSelfRecoveryOp(t, srCrashTarget)}
	r.donorIdx = nodeIndex(t, r.op.source)
	r.controlIdx = 3 - r.donorIdx - srCrashWipedIdx
	r.controlURI = compose.ContainerURI(r.controlIdx)
	helper.SetupClient(r.controlURI)
	t.Logf("op %s recovers %s/%s on %s from donor %s; control node %s",
		r.op.id, class, r.op.shard, r.target, r.op.source, srNodeNames[r.controlIdx])
	return r
}

func (r *srCrashRig) writeGap(t *testing.T, n int, cl types.ConsistencyLevel) {
	t.Helper()
	submitBatch(t, srParagraphObjects(r.class, srCrashGapPrefix, n, ""), cl)
}

// srCrashPresent lists base ids [baseFrom, baseTo) followed by the first gapCount gap ids.
func srCrashPresent(baseFrom, baseTo, gapCount int) []strfmt.UUID {
	return append(srIDs(srCrashBasePrefix, baseFrom, baseTo), srIDs(srCrashGapPrefix, 0, gapCount)...)
}

// Pins that a SELF_RECOVERY op whose target is SIGKILLed mid-HYDRATING resumes to READY instead of failing on the donor's leftover change log.
func TestSelfRecoveryCrashTargetMidHydratingResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	const baseCount, gapCount = 300, 50
	r := startSrCrashRig(ctx, t, "CrashHydPara", baseCount, map[string]string{"WEAVIATE_TEST_COPY_REPLICA_SLEEP": "20s"})

	mustRun(t, "op hydrates with an open donor change log", func(t *testing.T) {
		waitSelfRecoveryOpState(t, r.op.id, "HYDRATING", 3*time.Minute)
		require.Eventually(t, func() bool {
			return srChangelogPresent(ctx, t, r.compose, r.donorIdx, r.op.shard)
		}, 2*time.Minute, 200*time.Millisecond, "donor %s never opened a change log for %s", r.op.source, r.op.shard)
	})

	mustRun(t, "SIGKILL the target mid-HYDRATING and write while it is down", func(t *testing.T) {
		require.NoError(t, r.compose.KillNode(ctx, srCrashWipedIdx))
		helper.SetupClient(r.controlURI)
		r.writeGap(t, gapCount, types.ConsistencyLevelQuorum)
		require.NoError(t, r.compose.StartNode(ctx, srCrashWipedIdx))
		helper.SetupClient(r.controlURI)
	})

	mustRun(t, "op resumes to READY without file exists errors", func(t *testing.T) {
		d := waitSelfRecoveryOpState(t, r.op.id, "READY", 8*time.Minute)
		errs := srOpErrorsInState(d, "HYDRATING")
		t.Logf("op history %v; HYDRATING errors %v", srOpStates(d), errs)
		for _, msg := range errs {
			assert.NotContains(t, msg, "file exists")
		}
		assert.Less(t, len(errs), 10, "HYDRATING errors: %v", errs)
	})

	mustRun(t, "target holds the exact object set", func(t *testing.T) {
		helper.SetupClient(r.controlURI)
		waitNoShardRecovering(t, r.class, r.target)
		waitShardObjectCounts(t, r.class, r.target, baseCount+gapCount)
		assertExactObjectsOnNode(t, r.controlURI, r.class, r.target, srCrashPresent(0, baseCount, gapCount))
	})
}
