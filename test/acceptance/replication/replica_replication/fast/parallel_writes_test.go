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
	"fmt"
	mathrand "math/rand/v2"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/test/docker"
)

// parallelWriteResult is the writer's final source of truth: the exact
// object set every replica must hold after the op under test completes.
type parallelWriteResult struct {
	liveIDs    map[strfmt.UUID]string   // id → most-recent contents
	deletedIDs map[strfmt.UUID]struct{} // ids that were deleted
}

// startParallelWrites launches a single goroutine that round-robins CREATE
// (60%) / UPDATE (20%) / DELETE (20%) at CL=ALL across every node. A single
// serialized writer is deliberate: with one op in flight at a time the
// returned maps are an exact source of truth — no EC fudge.
//
// tenant is "" for single-tenant. seed pre-populates liveIDs.
//
// The returned stop func joins the writer and returns its final state.
func startParallelWrites(
	t *testing.T,
	compose *docker.DockerCompose,
	clusterSize int,
	class, tenant string,
	seed map[strfmt.UUID]string,
) (stop func() parallelWriteResult) {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()

	// liveIDs/deletedIDs are owned by the writer until stop()'s wg.Wait
	// establishes happens-before for the post-stop read.
	liveIDs := make(map[strfmt.UUID]string, len(seed))
	for id, contents := range seed {
		liveIDs[id] = contents
	}
	deletedIDs := map[strfmt.UUID]struct{}{}

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
				uri := compose.ContainerURI(containerID)
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
						assert.NoError(t, err, "error deleting object %s on node %s", target, uri)
						continue
					}
					delete(liveIDs, target)
					deletedIDs[target] = struct{}{}
				}
			}
		}
	}, logger)

	// Idempotent stop so callers can both `defer stopWrites()` and later
	// call `writes = stopWrites()` — the defer protects against a
	// require-driven Goexit leaving the writer racing on a defunct *testing.T.
	var stopOnce sync.Once
	return func() parallelWriteResult {
		stopOnce.Do(func() {
			close(done)
			wg.Wait()
		})
		return parallelWriteResult{liveIDs: liveIDs, deletedIDs: deletedIDs}
	}
}
