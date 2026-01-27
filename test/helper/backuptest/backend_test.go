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

package backuptest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockBackend is a test implementation of BackupBackend for validating
// the interface contracts and testing code that depends on BackupBackend.
type MockBackend struct {
	*BaseBackend
	name       string
	endpoint   string
	buckets    map[string]bool
	envVars    map[string]string
	startErr   error
	stopErr    error
	bucketErr  error
	startCalls int
	stopCalls  int
}

// NewMockBackend creates a new MockBackend for testing.
func NewMockBackend(opts ...BackendOption) *MockBackend {
	return &MockBackend{
		BaseBackend: NewBaseBackend(opts...),
		name:        "mock",
		endpoint:    "localhost:9999",
		buckets:     make(map[string]bool),
		envVars: map[string]string{
			"MOCK_ENDPOINT": "localhost:9999",
			"MOCK_BUCKET":   "backups",
		},
	}
}

// Compile-time check that MockBackend implements BackupBackend.
var _ BackupBackend = (*MockBackend)(nil)

func (m *MockBackend) Name() string {
	return m.name
}

func (m *MockBackend) Start(ctx context.Context) error {
	m.startCalls++
	if err := m.CheckNotStarted(); err != nil {
		return err
	}
	if m.startErr != nil {
		return m.startErr
	}
	m.SetState(StateStarting)
	// Simulate creating default bucket
	m.buckets[m.BucketName()] = true
	m.SetState(StateRunning)
	return nil
}

func (m *MockBackend) Stop(ctx context.Context) error {
	m.stopCalls++
	if m.State() == StateStopped {
		return nil // Idempotent
	}
	if m.stopErr != nil {
		return m.stopErr
	}
	m.SetState(StateStopping)
	m.buckets = make(map[string]bool)
	m.SetState(StateStopped)
	return nil
}

func (m *MockBackend) GetWeaviateEnv() map[string]string {
	if err := m.CheckRunning(); err != nil {
		return nil
	}
	return m.envVars
}

func (m *MockBackend) CreateBucket(ctx context.Context, name string) error {
	if err := m.CheckRunning(); err != nil {
		return err
	}
	if m.bucketErr != nil {
		return m.bucketErr
	}
	if m.buckets[name] {
		return ErrBucketExists
	}
	m.buckets[name] = true
	return nil
}

func (m *MockBackend) Endpoint() string {
	if err := m.CheckRunning(); err != nil {
		return ""
	}
	return m.endpoint
}

// Test helper methods for MockBackend

// SetStartError configures Start() to return the given error.
func (m *MockBackend) SetStartError(err error) {
	m.startErr = err
}

// SetStopError configures Stop() to return the given error.
func (m *MockBackend) SetStopError(err error) {
	m.stopErr = err
}

// SetBucketError configures CreateBucket() to return the given error.
func (m *MockBackend) SetBucketError(err error) {
	m.bucketErr = err
}

// HasBucket returns true if the bucket exists.
func (m *MockBackend) HasBucket(name string) bool {
	return m.buckets[name]
}

// StartCalls returns the number of times Start() was called.
func (m *MockBackend) StartCalls() int {
	return m.startCalls
}

// StopCalls returns the number of times Stop() was called.
func (m *MockBackend) StopCalls() int {
	return m.stopCalls
}

// =============================================================================
// Validation Tests
// =============================================================================

func TestMockBackend_ImplementsInterface(t *testing.T) {
	// This test validates that MockBackend satisfies the BackupBackend interface.
	// The compile-time check above ensures this, but this test makes it explicit.
	var backend BackupBackend = NewMockBackend()
	assert.NotNil(t, backend)
	assert.Equal(t, "mock", backend.Name())
}

func TestMockBackend_Lifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("normal lifecycle", func(t *testing.T) {
		backend := NewMockBackend()

		// Initial state
		assert.Equal(t, StateNotStarted, backend.State())
		assert.Equal(t, "", backend.Endpoint(), "Endpoint should be empty before start")
		assert.Nil(t, backend.GetWeaviateEnv(), "GetWeaviateEnv should return nil before start")

		// Start
		err := backend.Start(ctx)
		require.NoError(t, err)
		assert.Equal(t, StateRunning, backend.State())
		assert.Equal(t, 1, backend.StartCalls())

		// Verify running state
		assert.Equal(t, "localhost:9999", backend.Endpoint())
		assert.NotNil(t, backend.GetWeaviateEnv())
		assert.True(t, backend.HasBucket("backups"), "default bucket should exist")

		// Create additional bucket
		err = backend.CreateBucket(ctx, "override-bucket")
		require.NoError(t, err)
		assert.True(t, backend.HasBucket("override-bucket"))

		// Stop
		err = backend.Stop(ctx)
		require.NoError(t, err)
		assert.Equal(t, StateStopped, backend.State())
		assert.Equal(t, 1, backend.StopCalls())
	})

	t.Run("idempotent stop", func(t *testing.T) {
		backend := NewMockBackend()

		require.NoError(t, backend.Start(ctx))
		require.NoError(t, backend.Stop(ctx))

		// Second stop should not error
		err := backend.Stop(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 2, backend.StopCalls())
	})
}

func TestMockBackend_ContractViolations(t *testing.T) {
	ctx := context.Background()

	t.Run("GetWeaviateEnv before Start", func(t *testing.T) {
		backend := NewMockBackend()
		env := backend.GetWeaviateEnv()
		assert.Nil(t, env, "GetWeaviateEnv should return nil before Start")
	})

	t.Run("CreateBucket before Start", func(t *testing.T) {
		backend := NewMockBackend()
		err := backend.CreateBucket(ctx, "test-bucket")
		assert.ErrorIs(t, err, ErrNotStarted)
	})

	t.Run("Endpoint before Start", func(t *testing.T) {
		backend := NewMockBackend()
		endpoint := backend.Endpoint()
		assert.Empty(t, endpoint, "Endpoint should be empty before Start")
	})

	t.Run("Start twice", func(t *testing.T) {
		backend := NewMockBackend()
		require.NoError(t, backend.Start(ctx))

		err := backend.Start(ctx)
		assert.ErrorIs(t, err, ErrAlreadyStarted)
		assert.Equal(t, 2, backend.StartCalls())
	})

	t.Run("Start after Stop", func(t *testing.T) {
		backend := NewMockBackend()
		require.NoError(t, backend.Start(ctx))
		require.NoError(t, backend.Stop(ctx))

		err := backend.Start(ctx)
		assert.ErrorIs(t, err, ErrAlreadyStopped)
	})

	t.Run("CreateBucket after Stop", func(t *testing.T) {
		backend := NewMockBackend()
		require.NoError(t, backend.Start(ctx))
		require.NoError(t, backend.Stop(ctx))

		err := backend.CreateBucket(ctx, "test-bucket")
		assert.ErrorIs(t, err, ErrAlreadyStopped)
	})

	t.Run("CreateBucket with existing name", func(t *testing.T) {
		backend := NewMockBackend()
		require.NoError(t, backend.Start(ctx))

		// Default bucket already exists
		err := backend.CreateBucket(ctx, "backups")
		assert.ErrorIs(t, err, ErrBucketExists)
	})
}

func TestBackendOptions(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		backend := NewMockBackend()
		assert.Equal(t, "backups", backend.BucketName())
	})

	t.Run("custom bucket name", func(t *testing.T) {
		backend := NewMockBackend(WithBucketName("custom-bucket"))
		assert.Equal(t, "custom-bucket", backend.BucketName())
	})

	t.Run("custom network name", func(t *testing.T) {
		backend := NewMockBackend(WithNetworkName("test-network"))
		assert.Equal(t, "test-network", backend.Config().networkName)
	})

	t.Run("disable container reuse", func(t *testing.T) {
		backend := NewMockBackend(WithContainerReuse(false))
		assert.False(t, backend.Config().containerReuse)
	})

	t.Run("multiple options", func(t *testing.T) {
		backend := NewMockBackend(
			WithBucketName("my-bucket"),
			WithNetworkName("my-network"),
			WithContainerReuse(false),
		)
		assert.Equal(t, "my-bucket", backend.BucketName())
		assert.Equal(t, "my-network", backend.Config().networkName)
		assert.False(t, backend.Config().containerReuse)
	})
}

func TestBackendState_String(t *testing.T) {
	tests := []struct {
		state    BackendState
		expected string
	}{
		{StateNotStarted, "not_started"},
		{StateStarting, "starting"},
		{StateRunning, "running"},
		{StateStopping, "stopping"},
		{StateStopped, "stopped"},
		{BackendState(99), "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestBaseBackend_CheckRunning(t *testing.T) {
	tests := []struct {
		state       BackendState
		expectedErr error
	}{
		{StateNotStarted, ErrNotStarted},
		{StateStarting, ErrNotStarted},
		{StateRunning, nil},
		{StateStopping, ErrAlreadyStopped},
		{StateStopped, ErrAlreadyStopped},
	}

	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			base := NewBaseBackend()
			base.SetState(tt.state)
			err := base.CheckRunning()
			if tt.expectedErr != nil {
				assert.ErrorIs(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBaseBackend_CheckNotStarted(t *testing.T) {
	tests := []struct {
		state       BackendState
		expectedErr error
	}{
		{StateNotStarted, nil},
		{StateStarting, ErrAlreadyStarted},
		{StateRunning, ErrAlreadyStarted},
		{StateStopping, ErrAlreadyStopped},
		{StateStopped, ErrAlreadyStopped},
	}

	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			base := NewBaseBackend()
			base.SetState(tt.state)
			err := base.CheckNotStarted()
			if tt.expectedErr != nil {
				assert.ErrorIs(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
