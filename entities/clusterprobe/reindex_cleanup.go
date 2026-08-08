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

package clusterprobe

import "fmt"

// ReindexCleanupActivity is the answer to "have you processed the cancel yet".
// One type for both ends of the wire: the handler marshals it and the client
// unmarshals it, so a tag change cannot land on one side only.
//
// CleaningUp is a pointer so a payload that never mentions it is rejected
// rather than read as "no cleanup here". Probe is what tells the caller the
// answer came from a node at all; see [ReindexCleanupMarker].
type ReindexCleanupActivity struct {
	Probe      string `json:"probe"`
	CleaningUp *bool  `json:"cleaningUp"`
}

// NewReindexCleanupActivity builds the answer a node sends, with the marker
// already set.
func NewReindexCleanupActivity(cleaningUp bool) ReindexCleanupActivity {
	return ReindexCleanupActivity{Probe: ReindexCleanupMarker, CleaningUp: &cleaningUp}
}

// InProgress reads the answer, refusing anything that cannot mean the node is
// free: a wrong or missing marker, or a payload that omits the field. Both are
// errors rather than a permissive default, because the permissive value clears
// the whole cluster at once.
func (a ReindexCleanupActivity) InProgress() (bool, error) {
	if a.Probe != ReindexCleanupMarker {
		return false, fmt.Errorf("answer is marked %q, want %q: this 200 did not come from a "+
			"Weaviate node, so it cannot mean the node is free; check for an HTTP proxy on the "+
			"cluster port", a.Probe, ReindexCleanupMarker)
	}
	if a.CleaningUp == nil {
		return false, fmt.Errorf("answer has no %q field, so it cannot mean the node is free", "cleaningUp")
	}
	return *a.CleaningUp, nil
}
