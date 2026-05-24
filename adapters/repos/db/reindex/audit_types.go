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

// KnownReindexTaskLookup reports whether (taskID, taskVersion) is
// "live" in the DTM scheduler snapshot. A KnownReindexTaskLookup
// reflects a snapshot taken at the START of an audit by a
// [KnownReindexTaskLookupBuilder] so the audit's per-tracker
// classification consults one consistent snapshot instead of issuing
// a RAFT read per tracker (Copilot review).
type KnownReindexTaskLookup func(taskID string, taskVersion uint64) bool

// KnownReindexTaskLookupBuilder returns a fresh KnownReindexTaskLookup
// for one audit invocation. The audit calls it once at entry and
// reuses the returned lookup for the entire walk.
type KnownReindexTaskLookupBuilder func() KnownReindexTaskLookup
