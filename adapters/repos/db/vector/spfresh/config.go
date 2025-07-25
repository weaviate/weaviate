//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

// UserConfig defines the configuration options for the SPFresh index.
type UserConfig struct {
	MaxPostingSize         uint32 `json:"maxPostingSize,omitempty"`         // Maximum number of vectors in a posting
	MinPostingSize         uint32 `json:"minPostingSize,omitempty"`         // Minimum number of vectors in a posting
	SplitWorkers           int    `json:"splitWorkers,omitempty"`           // Number of concurrent workers for split operations
	ReassignWorkers        int    `json:"reassignWorkers,omitempty"`        // Number of concurrent workers for reassign operations
	MergePostingCandidates int    `json:"mergePostingCandidates,omitempty"` // Number of candidates to consider for merging postings
}
