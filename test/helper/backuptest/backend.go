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

// Package backuptest provides self-contained test infrastructure for backup
// integration tests. It handles automatic provisioning and cleanup of storage
// backend emulators (MinIO for S3, GCS emulator, Azurite for Azure) with zero
// required configuration.
package backuptest

import (
	"context"
	"errors"
	"fmt"
)

// BackupBackend encapsulates all setup/teardown for a backup storage backend.
// Implementations handle container lifecycle, bucket/container creation,
// and provide environment variables for Weaviate configuration.
type BackupBackend interface {
	// Name returns the backend identifier used by Weaviate (s3, gcs, azure).
	Name() string

	// Start provisions all required infrastructure:
	// - Starts the emulator container (MinIO, GCS emulator, or Azurite)
	// - Creates the default bucket/container
	// - Waits for the service to be healthy
	// Returns an error if provisioning fails.
	Start(ctx context.Context) error

	// Stop tears down all infrastructure created by Start.
	// This should be called in a defer after Start succeeds.
	// It's safe to call Stop multiple times.
	Stop(ctx context.Context) error

	// GetWeaviateEnv returns environment variables that Weaviate needs to
	// connect to this backend. These should be passed to the Weaviate container.
	// Must only be called after Start() succeeds.
	GetWeaviateEnv() map[string]string

	// BucketName returns the default bucket/container name used for backups.
	BucketName() string

	// CreateBucket creates an additional bucket/container for override testing.
	// Must only be called after Start() succeeds.
	CreateBucket(ctx context.Context, name string) error

	// Endpoint returns the emulator's endpoint URI for external access.
	// Must only be called after Start() succeeds.
	Endpoint() string
}

// BackendState tracks the lifecycle state of a backend.
type BackendState int

const (
	// StateNotStarted is the initial state before Start() is called.
	StateNotStarted BackendState = iota
	// StateStarting indicates Start() is in progress.
	StateStarting
	// StateRunning indicates the backend is fully operational.
	StateRunning
	// StateStopping indicates Stop() is in progress.
	StateStopping
	// StateStopped indicates the backend has been stopped.
	StateStopped
)

func (s BackendState) String() string {
	switch s {
	case StateNotStarted:
		return "not_started"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateStopped:
		return "stopped"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// Common errors returned by BackupBackend implementations.
var (
	// ErrNotStarted is returned when methods requiring a running backend
	// are called before Start() succeeds.
	ErrNotStarted = errors.New("backend not started")

	// ErrAlreadyStarted is returned when Start() is called on an already
	// running backend.
	ErrAlreadyStarted = errors.New("backend already started")

	// ErrAlreadyStopped is returned when operations are attempted on a
	// stopped backend.
	ErrAlreadyStopped = errors.New("backend already stopped")

	// ErrBucketExists is returned when CreateBucket is called with a
	// bucket name that already exists.
	ErrBucketExists = errors.New("bucket already exists")
)

// BackendOption configures a BackupBackend.
type BackendOption func(*backendConfig)

type backendConfig struct {
	bucketName     string
	networkName    string
	containerReuse bool
}

func defaultBackendConfig() *backendConfig {
	return &backendConfig{
		bucketName:     "backups",
		networkName:    "",
		containerReuse: true,
	}
}

// WithBucketName sets the default bucket/container name.
// Default is "backups".
func WithBucketName(name string) BackendOption {
	return func(c *backendConfig) {
		c.bucketName = name
	}
}

// WithNetworkName sets the Docker network name to use.
// If empty, a new network will be created.
func WithNetworkName(name string) BackendOption {
	return func(c *backendConfig) {
		c.networkName = name
	}
}

// WithContainerReuse enables/disables container reuse.
// When enabled, existing containers with matching names will be reused.
// Default is true.
func WithContainerReuse(reuse bool) BackendOption {
	return func(c *backendConfig) {
		c.containerReuse = reuse
	}
}

// BaseBackend provides common functionality for all backend implementations.
// Embed this in concrete backend types.
type BaseBackend struct {
	config *backendConfig
	state  BackendState
}

// NewBaseBackend creates a new BaseBackend with the given options.
func NewBaseBackend(opts ...BackendOption) *BaseBackend {
	cfg := defaultBackendConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return &BaseBackend{
		config: cfg,
		state:  StateNotStarted,
	}
}

// State returns the current backend state.
func (b *BaseBackend) State() BackendState {
	return b.state
}

// SetState updates the backend state.
func (b *BaseBackend) SetState(s BackendState) {
	b.state = s
}

// Config returns the backend configuration.
func (b *BaseBackend) Config() *backendConfig {
	return b.config
}

// BucketName returns the configured bucket name.
func (b *BaseBackend) BucketName() string {
	return b.config.bucketName
}

// CheckRunning returns ErrNotStarted if the backend is not in StateRunning.
func (b *BaseBackend) CheckRunning() error {
	switch b.state {
	case StateRunning:
		return nil
	case StateNotStarted, StateStarting:
		return ErrNotStarted
	case StateStopping, StateStopped:
		return ErrAlreadyStopped
	default:
		return fmt.Errorf("unexpected backend state: %s", b.state)
	}
}

// CheckNotStarted returns ErrAlreadyStarted if the backend is already running.
func (b *BaseBackend) CheckNotStarted() error {
	switch b.state {
	case StateNotStarted:
		return nil
	case StateRunning, StateStarting:
		return ErrAlreadyStarted
	case StateStopping, StateStopped:
		return ErrAlreadyStopped
	default:
		return fmt.Errorf("unexpected backend state: %s", b.state)
	}
}
