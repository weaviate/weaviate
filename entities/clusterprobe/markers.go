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

// Package clusterprobe holds the wire contract of the cluster-internal probes.
package clusterprobe

// Frozen across releases. Rewording the path fails open, since a peer 404s the
// new one and a 404 reads as "too old to ask"; rewording the marker fails closed.
const (
	BackupNodeActivityPath   = "/backups/node-activity"
	BackupNodeActivityMarker = "weaviate/backup-node-activity"
)

// What a build predating a probe route answers there: net/http's own 404, down to
// the newline it appends. Relaxing either lets a proxy's 404 read as a node's.
const (
	NodeNotFoundBody        = "404 page not found\n"
	NodeNotFoundHeader      = "X-Content-Type-Options"
	NodeNotFoundHeaderValue = "nosniff"
)
