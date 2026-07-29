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

package reindex

import (
	"encoding/json"

	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// DecodeErrorPolicy controls how [FirstActiveReindexTask] treats a task
// whose payload cannot be decoded.
type DecodeErrorPolicy int

const (
	// DecodeSkip ignores an undecodable task (the submit-time conflict
	// gate flags it, so the match sites treat it as "no task").
	DecodeSkip DecodeErrorPolicy = iota
	// DecodeUndecodableIsHit counts an undecodable task as a match — the
	// unverifiable scan fails closed rather than derive a trustworthy
	// NO_OP.
	DecodeUndecodableIsHit
)

// FirstActiveReindexTask returns the first active task whose decoded
// payload satisfies match. On a decode error, policy decides: DecodeSkip
// continues, DecodeUndecodableIsHit returns that task as a match.
// Factors the shared IsActive + unmarshal loop out of the per-index
// lookup helpers.
func FirstActiveReindexTask(
	tasks []*distributedtask.Task,
	policy DecodeErrorPolicy,
	match func(dbreindex.ReindexTaskPayload) bool,
) (*distributedtask.Task, dbreindex.ReindexTaskPayload, bool) {
	for _, t := range tasks {
		if !t.Status.IsActive() {
			continue
		}
		var p dbreindex.ReindexTaskPayload
		if err := json.Unmarshal(t.Payload, &p); err != nil {
			if policy == DecodeUndecodableIsHit {
				return t, dbreindex.ReindexTaskPayload{}, true
			}
			continue
		}
		if match(p) {
			return t, p, true
		}
	}
	return nil, dbreindex.ReindexTaskPayload{}, false
}
