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

// Package clusterprobe holds the wire markers of the read-only
// cluster-internal probes. It is a leaf so that the handler serving a probe
// and the client reading it can agree on one value without importing each
// other.
package clusterprobe

// Each probe answers "nothing running here", and that answer is what lets the
// reindex gate admit a migration. Only an answer that proves it came from a
// node may say it: without a marker, every JSON object an intermediary can
// return in a node's stead — a transparent proxy, a misrouted ingress —
// decodes to the permissive value and clears the whole cluster at once. Each
// route puts its own marker in the response's "probe" field, and its client
// refuses a 200 that carries anything else.
const (
	BackupNodeActivityMarker = "weaviate/backup-node-activity"
	ReindexCleanupMarker     = "weaviate/reindex-cleanup-activity"
)

// The route each probe is served on, defined once for the mux that mounts it
// and the client that calls it. A path that disagreed between the two would
// answer 404, which is exactly what the clients read as "this node runs an
// older build" and let through — a typo would silently disable the gate rather
// than break it.
const (
	BackupNodeActivityPath     = "/backups/node-activity"
	ReindexCleanupActivityPath = "/reindex/cleanup-activity"
)
