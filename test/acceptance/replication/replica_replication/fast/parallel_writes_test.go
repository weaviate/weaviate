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

package replication

import (
	"context"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// parallelWriteResult is the writer's final source of truth once it has
// stopped: the exact object set every replica of the collection must hold
// after the replication op under test completes.
type parallelWriteResult struct {
	// liveIDs maps object id -> the contents string of its most recent write.
	liveIDs map[strfmt.UUID]string
	// deletedIDs is the set of ids the workload created (or seeded) and then
	// deleted — they must be absent from every replica.
	deletedIDs map[strfmt.UUID]struct{}
}

// startParallelWrites launches a single background goroutine that round-robins
// CREATE (60%) / UPDATE (20%) / DELETE (20%) operations across every node in
// the cluster, each at consistency level ALL, until the returned stop func is
// called.
//
// It is the shared workload behind the parallel-write replication tests
// (TestReplicaMovementTenantParallelWrites for MOVE on a multi-tenant
// collection, TestReplicaMovementShardScaleOutParallelWrites for COPY scale-out
// on a single-tenant collection). A *single, serialized* writer is deliberate:
// because only one operation is in flight at a time and each is acked at
// CL=ALL before the next begins, the returned liveIDs/deletedIDs maps are an
// exact source of truth for what every replica must hold once the op under
// test finishes — no eventual-consistency fudge needed.
//
// tenant is "" for single-tenant collections. seed pre-populates liveIDs with
// the objects that already exist before writes start, so the workload is free
// to UPDATE/DELETE pre-existing data too.
//
// The returned stop func closes the writer, waits for it to exit, and returns
// its final tracked state. Call it only after the replication op under test
// has reached READY.
// nodeSourcer abstracts the per-node lookups the parallel writer + dumpers
// need so the same helpers work against either testcontainers (production
// path) or a long-running local docker-compose stack (fast-iteration path).
// Implementations are tiny — see composeNodeSource / localNodeSource in the
// scale-out test for the two flavours.
type nodeSourcer interface {
	// Size is the number of weaviate nodes in the cluster.
	Size() int
	// URIFor returns the host:port REST URI for the i-th node (1-indexed,
	// matching the existing compose.ContainerURI convention).
	URIFor(i int) string
	// FetchLogs returns the full container log for the i-th weaviate node
	// (1-indexed). The caller owns the returned reader and must Close it.
	FetchLogs(ctx context.Context, i int) (io.ReadCloser, error)
}

func startParallelWrites(
	t *testing.T,
	ctx context.Context,
	nodes nodeSourcer,
	class, tenant string,
	seed map[strfmt.UUID]string,
) (stop func() parallelWriteResult) {
	clusterSize := nodes.Size()
	t.Helper()
	logger, _ := logrustest.NewNullLogger()

	// liveIDs/deletedIDs are owned exclusively by the writer goroutine until
	// stop() joins it; the happens-before from wg.Wait() makes the post-stop
	// read race-free.
	liveIDs := make(map[strfmt.UUID]string, len(seed))
	for id, contents := range seed {
		liveIDs[id] = contents
	}
	deletedIDs := map[strfmt.UUID]struct{}{}

	// dumpLogsOnce gates dumpReplicaLogsOnce so the first parallel-write
	// failure dumps the relevant log lines from every node and subsequent
	// failures stay quiet.
	var dumpLogsOnce sync.Once
	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		containerID := 1
		// opSeq keeps every CREATE / UPDATE content string distinct so a stale
		// PUT winning over a newer one shows up as a content-equality failure.
		opSeq := 0
		// pickLive returns a random id from liveIDs, or "" if empty.
		pickLive := func() strfmt.UUID {
			if len(liveIDs) == 0 {
				return ""
			}
			idx := mathrand.IntN(len(liveIDs))
			i := 0
			for id := range liveIDs {
				if i == idx {
					return id
				}
				i++
			}
			return ""
		}
		for {
			select {
			case <-done:
				return
			default:
				opSeq++
				uri := nodes.URIFor(containerID)
				containerID++
				if containerID >= clusterSize+1 {
					containerID = 1
				}

				// 60% CREATE / 20% UPDATE / 20% DELETE. Force CREATE if the
				// live-set is empty (cannot UPDATE/DELETE nothing).
				roll := mathrand.IntN(100)
				switch {
				case roll < 60 || len(liveIDs) == 0:
					newID := strfmt.UUID(uuid.New().String())
					contents := fmt.Sprintf("paragraph#%d", opSeq)
					if err := createObjectThreadSafe(uri, class, map[string]any{"contents": contents}, string(newID), tenant); err != nil {
						dumpReplicaLogsOnce(t, ctx, nodes, &dumpLogsOnce)
						assert.NoError(t, err, "error creating object %s on node %s", newID, uri)
						continue
					}
					liveIDs[newID] = contents
				case roll < 80:
					target := pickLive()
					if target == "" {
						continue
					}
					contents := fmt.Sprintf("paragraph#%d-updated-%d", opSeq, opSeq)
					if err := patchObjectThreadSafe(uri, class, string(target), tenant, map[string]any{"contents": contents}); err != nil {
						dumpReplicaLogsOnce(t, ctx, nodes, &dumpLogsOnce)
						assert.NoError(t, err, "error patching object %s on node %s", target, uri)
						continue
					}
					liveIDs[target] = contents
				default:
					target := pickLive()
					if target == "" {
						continue
					}
					if err := deleteObjectThreadSafe(uri, class, string(target), tenant); err != nil {
						dumpReplicaLogsOnce(t, ctx, nodes, &dumpLogsOnce)
						assert.NoError(t, err, "error deleting object %s on node %s", target, uri)
						continue
					}
					delete(liveIDs, target)
					deletedIDs[target] = struct{}{}
				}
			}
		}
	}, logger)

	// The returned stop is idempotent — the close+wait runs at most once,
	// so callers can both `defer stopWrites()` immediately after this
	// returns AND later call `writes = stopWrites()` to capture the
	// writer's final state. The defer is what prevents a require-driven
	// Goexit from leaving the writer running past the test's lifetime
	// and racing on the now-defunct *testing.T.
	var stopOnce sync.Once
	return func() parallelWriteResult {
		stopOnce.Do(func() {
			close(done)
			wg.Wait()
		})
		return parallelWriteResult{liveIDs: liveIDs, deletedIDs: deletedIDs}
	}
}
