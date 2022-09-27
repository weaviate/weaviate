package backup

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/backup"
)

type client interface {
	// CanCommit ask a node if it can participate in a distributed backup operation
	CanCommit(ctx context.Context, node string, req *Request) (CanCommitResponse, error)
	// Commit tells a node to commit its part
	Commit(ctx context.Context, node string, _ *StatusRequest) error
	// Status returns the status of a backup operation of a specific node
	Status(_ context.Context, node string, _ *StatusRequest) (*StatusResponse, error)
	// Abort tells a node to abort the previous backup operation
	Abort(_ context.Context, node string, _ *AbortRequest) (AbortResponse, error)
}

type Request struct {
	// Method is the backup operation (create, restore)
	Method Op
	// ID is the backup ID
	ID string
	// Backend specify on which backend to store backups (gcs, s3, ..)
	Backend string

	// Classes is list of class which need to be backed up
	Classes []string

	// Duration in milliseconds
	Duration int
}

type CanCommitResponse struct {
	// Method is the backup operation (create, restore)
	Method Op
	// ID is the backup ID
	ID string
	// Timeout for how long the promise might be hold
	Timeout int
}

type StatusRequest struct {
	// Method is the backup operation (create, restore)
	Method Op
	ID     string
}

type StatusResponse struct {
	// Method is the backup operation (create, restore)
	Method Op
	ID     string
	Status backup.Status
	Err    string
}

type (
	AbortRequest  StatusRequest
	AbortResponse StatusResponse
)
