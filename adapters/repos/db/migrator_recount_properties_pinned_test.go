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

//go:build integrationTest

package db

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// Pins a defect of RECOUNT_PROPERTIES_AT_STARTUP, and fails until it is fixed.
//
// The server calls RecountProperties without waiting for the db to load its
// indices. It then finds none, recounts nothing, and reports the recount as
// complete.
func TestRecountProperties_BeforeStartupCompleted(t *testing.T) {
	repo := newUnstartedTestDB(t)
	logger, _ := test.NewNullLogger()

	err := NewMigrator(repo, logger, "node1").RecountProperties(testCtx())
	require.Error(t, err, "no index was loaded yet, so nothing was recounted, and that must not pass for success")
}
