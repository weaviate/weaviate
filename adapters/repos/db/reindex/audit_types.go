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

// KnownReindexTaskLookupBuilder returns a fresh [KnownReindexTaskLookup]
// for one audit invocation. Returns an error when the underlying DTM
// snapshot cannot be obtained (e.g. ListDistributedTasks is timing out
// during a network partition). Callers MUST propagate the error rather
// than substitute a soft default — an unobservable "all known" fallback
// would silently misclassify orphans as in-flight migrations.
type KnownReindexTaskLookupBuilder func() (KnownReindexTaskLookup, error)
