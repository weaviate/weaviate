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
	"encoding/json"
	"fmt"
)

// DropVectorIndexNamespace is the distributed-task namespace for dropping a
// named vector index. The DTM Manager routes tasks with this namespace to the
// DropVectorIndexProvider.
const DropVectorIndexNamespace = "drop-vector-index"

// DropVectorIndexTaskPayload is the RAFT-replicated payload of a drop-vector
// task: the collection, the dropped named vectors (C3: several at once), the
// edit-ops bookkeeping key (OpID), and the unit→node/unit→shard assignment.
type DropVectorIndexTaskPayload struct {
	Collection string   `json:"collection"`
	Targets    []string `json:"targets"`
	OpID       string   `json:"opId"`

	// UnitToNode maps a unit ID to the node that owns it; UnitToShard maps the
	// same unit ID to the shard it covers. One unit per (shard, node).
	UnitToNode  map[string]string `json:"unitToNode"`
	UnitToShard map[string]string `json:"unitToShard"`
}

func (p *DropVectorIndexTaskPayload) encode() ([]byte, error) {
	return json.Marshal(p)
}

func decodeDropVectorIndexPayload(data []byte) (*DropVectorIndexTaskPayload, error) {
	var p DropVectorIndexTaskPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal drop-vector-index payload: %w", err)
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("drop-vector-index payload missing collection")
	}
	if len(p.Targets) == 0 {
		return nil, fmt.Errorf("drop-vector-index payload missing targets")
	}
	for _, t := range p.Targets {
		// An empty target reaches os.RemoveAll via removeVectorIndexFiles; reject it.
		if t == "" {
			return nil, fmt.Errorf("drop-vector-index payload has an empty target name")
		}
	}
	if p.OpID == "" {
		return nil, fmt.Errorf("drop-vector-index payload missing opId")
	}
	return &p, nil
}

// ExtractDropVectorIndexTaskCollection is the collection extractor registered
// with the DTM Manager so the DeleteClass cascade can drop this namespace's task
// records (mirrors ExtractReindexTaskCollection). ok is false on an unparseable
// payload.
func ExtractDropVectorIndexTaskCollection(payload []byte) (collection string, ok bool) {
	p, err := decodeDropVectorIndexPayload(payload)
	if err != nil {
		return "", false
	}
	return p.Collection, true
}
