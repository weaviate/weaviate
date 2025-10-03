//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest_test

import (
	"sync"
	"testing"

	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest"
)

func TestShutdownCoordinatorDefaultState(t *testing.T) {
	// GIVEN
	logger, _ := logrusTest.NewNullLogger()
	sc := rest.NewShutdownCoordinator(logger)

	// THEN
	require.False(t, sc.IsShuttingDown(), "shutdown should be false by default")
}

func TestShutdownCoordinatorNotified(t *testing.T) {
	// GIVEN
	logger, _ := logrusTest.NewNullLogger()
	sc := rest.NewShutdownCoordinator(logger)

	// WHEN
	sc.NotifyShutdown()

	// THEN
	require.True(t, sc.IsShuttingDown(), "shutdown should be true after notification")
}

func TestShutdownCoordinatorMultipleNotifications(t *testing.T) {
	// GIVEN
	logger, _ := logrusTest.NewNullLogger()
	sc := rest.NewShutdownCoordinator(logger)

	// WHEN
	for i := 0; i < 10; i++ {
		sc.NotifyShutdown()
	}

	// THEN
	require.True(t, sc.IsShuttingDown(), "shutdown should be true after multiple notifications")
}

func TestShutdownCoordinatorConcurrentNotifications(t *testing.T) {
	// GIVEN
	logger, _ := logrusTest.NewNullLogger()
	sc := rest.NewShutdownCoordinator(logger)

	// WHEN
	var wg sync.WaitGroup
	wg.Add(12)
	for i := 0; i < 12; i++ {
		go func() {
			defer wg.Done()
			sc.NotifyShutdown()
		}()
	}
	wg.Wait()

	// THEN
	require.True(t, sc.IsShuttingDown(), "shutdown should be true after concurrent notifications")
}
