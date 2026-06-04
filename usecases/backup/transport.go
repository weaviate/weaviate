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
