//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package state_test

import (
	"sync/atomic"
	"testing"

	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

// fake implementing the consumer-defined (unexported) interface.
type fakeShutdownTracker struct{ v int32 }

func (f *fakeShutdownTracker) IsShuttingDown() bool {
	return atomic.LoadInt32(&f.v) == 1
}

func (f *fakeShutdownTracker) set(b bool) {
	if b {
		atomic.StoreInt32(&f.v, 1)
	} else {
		atomic.StoreInt32(&f.v, 0)
	}
}

func newTestState() (*state.State, *logrusTest.Hook) {
	logger, hook := logrusTest.NewNullLogger()
	return &state.State{Logger: logger}, hook
}

func TestStateIsShuttingDown_NilTrackers(t *testing.T) {
	// GIVEN
	testState, _ := newTestState()

	// THEN
	require.False(t, testState.IsShuttingDown(), "no trackers set => not shutting down")
}

func TestStateIsShuttingDown_WithFakeTrackers_Toggling(t *testing.T) {
	// GIVEN
	testState, _ := newTestState()
	var restFake, grpcFake fakeShutdownTracker

	// WHEN
	testState.SetShutdownRestTracker(&restFake)
	testState.SetShutdownGrpcTracker(&grpcFake)

	// THEN (initially both false)
	require.False(t, testState.IsShuttingDown())

	// WHEN (REST true)
	restFake.set(true)
	require.True(t, testState.IsShuttingDown(), "rest true and grpc false should trigger shutdown")

	// WHEN (REST false again)
	restFake.set(false)
	require.False(t, testState.IsShuttingDown(), "rest false and grpc false should not trigger shutdown")

	// WHEN (gRPC true)
	grpcFake.set(true)
	require.True(t, testState.IsShuttingDown(), "grpc true should trigger shutdown")

	// WHEN (both true)
	restFake.set(true)
	require.True(t, testState.IsShuttingDown(), "rest and grpc true should trigger shutdown")

	// WHEN (both false)
	restFake.set(false)
	grpcFake.set(false)
	require.False(t, testState.IsShuttingDown(), "rest and grpc false should not trigger shutdown")
}
