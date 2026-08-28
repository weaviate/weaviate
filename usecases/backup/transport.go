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

package backup

import (
	"context"
	"time"

	"github.com/weaviate/weaviate/entities/backup"
)

type client interface {
	// CanCommit ask a node if it can participate in a distributed backup operation
	CanCommit(ctx context.Context, node string, req *Request) (*CanCommitResponse, error)
	// Commit tells a node to commit its part
	Commit(ctx context.Context, node string, _ *StatusRequest) error
	// Status returns the status of a backup operation of a specific node
	Status(_ context.Context, node string, _ *StatusRequest) (*StatusResponse, error)
	// Abort tells a node to abort the previous backup operation
	Abort(_ context.Context, node string, _ *AbortRequest) error
}

type Request struct {
	// Method is the backup operation (create, restore)
	Method Op
	// ID is the backup ID
	ID string
	// Backend specify on which backend to store backups (gcs, s3, ..)
	Backend string

	// NodeMapping specify node names replacement to be made on restore
	NodeMapping map[string]string

	// Classes is list of class which need to be backed up
	Classes []string

	// Resolved from BackupRequest.IncludeUsers by the scheduler. Empty
	// means the participant keeps its whole-cluster user-snapshot default.
	Users []string

	// Resolved from BackupRequest.IncludeRoles by the scheduler. Empty
	// means the participant keeps its whole-cluster RBAC-snapshot default.
	Roles []string

	// Duration
	Duration time.Duration

	// Compression is the compression configuration.
	Compression

	// Override bucket
	Bucket string

	// Additional path prefix override
	Path string

	// NodeName is the target node name for this backup operation
	NodeName string
	// NodeHost is the target node's hostname for this backup operation
	NodeHost string

	RbacRestoreOption string
	UserRestoreOption string

	RestoreOverwriteAlias bool

	BaseBackupID string

	// DedupeReplicas marks a replica-deduped backup (create: opt-in echo, restore: fan-out marker); older nodes ignore it.
	DedupeReplicas bool `json:"dedupeReplicas,omitempty"`

	// DedupeEffective is the planning outcome (designated shards exist) and alone drives stamping; DedupeReplicas keeps carrying the request flag for the capability guard.
	DedupeEffective bool `json:"dedupeEffective,omitempty"`

	// DedupeConvergenceTimeoutSeconds bounds convergence planning, coordinator-side only; 0 = default.
	DedupeConvergenceTimeoutSeconds int `json:"dedupeConvergenceTimeoutSeconds,omitempty"`

	// ShardDesignations (class -> shard -> archiving node) EXCLUDES: a participant skips a shard only when a DIFFERENT node is named, so drift degrades to duplication, never omission.
	ShardDesignations map[string]map[string]string `json:"shardDesignations,omitempty"`

	// SourceNodes are the original node names whose {backupID}/{node} subtrees hold descriptors and chunks.
	SourceNodes []string `json:"sourceNodes,omitempty"`

	// AttemptID distinguishes coordinator attempts sharing a backup ID; older nodes ignore it.
	AttemptID string `json:"attemptId,omitempty"`
}

// CanCommitErrorKind is a coarse, JSON-stable classification of a remote
// canCommit failure. It is the structured companion to the free-form Err
// message: handlers set it on the response, coordinators map it back to a
// typed sentinel without needing to import the originating package.
//
// Unknown / empty values from older nodes are treated as
// [CanCommitErrCannotCommit].
type CanCommitErrorKind string

const (
	// CanCommitErrInFlightReindex indicates the participant refused because
	// a runtime-reindex tracker is in flight on one of the requested shards.
	// The coordinator translates this to a typed
	// ErrBackupBlockedByInFlightReindex on receipt.
	CanCommitErrInFlightReindex CanCommitErrorKind = "in_flight_reindex"

	// CanCommitErrCannotCommit is the generic fallback used when the
	// participant rejected canCommit for any reason other than the
	// classified kinds above.
	CanCommitErrCannotCommit CanCommitErrorKind = "cannot_commit"
)

type CanCommitResponse struct {
	// Method is the backup operation (create, restore)
	Method Op
	// ID is the backup ID
	ID string
	// Timeout for how long the promise might be hold
	Timeout time.Duration
	// Err error
	Err string
	// ErrKind is a structured classification of Err. Empty when Err is
	// empty. Older nodes never set this field; consumers must treat the
	// zero value as [CanCommitErrCannotCommit].
	ErrKind CanCommitErrorKind `json:"err_kind,omitempty"`
	// DedupeHonored acks DedupeReplicas support; older nodes never set it and the coordinator aborts without it.
	DedupeHonored bool `json:"dedupe_honored,omitempty"`
}

type StatusRequest struct {
	// Method is the backup operation (create, restore)
	Method Op
	// ID is the backup ID
	ID string
	// Backend specify on which backend to store backups (gcs, s3, ..)
	Backend string
	// Bucket specify the bucket name
	Bucket string
	// Path specify the path
	Path string

	BaseBackupID string

	// AttemptID gates which coordinator attempt an abort may cancel; empty means legacy ID-only matching.
	AttemptID string `json:"attemptId,omitempty"`
}

type StatusResponse struct {
	// Method is the backup operation (create, restore)
	Method Op
	ID     string
	Status backup.Status
	Err    string
}

type (
	AbortRequest StatusRequest
)
