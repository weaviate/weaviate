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

package cluster

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

// TestNewTxSlowLogThresholds pins the env-var threshold parsing in newTxSlowLog:
// the presence guard (`!= ""`) and the parse-success guard (`err == nil`) must
// each gate whether the override is applied.
func TestNewTxSlowLogThresholds(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("defaults when env is unset", func(t *testing.T) {
		t.Setenv("TX_SLOW_LOG_AGE_THRESHOLD_SECONDS", "")
		t.Setenv("TX_SLOW_LOG_CHANGE_THRESHOLD_SECONDS", "")
		sl := newTxSlowLog(logger)
		assert.Equal(t, 5*time.Second, sl.ageThreshold)
		assert.Equal(t, 1*time.Second, sl.changeThreshold)
	})

	t.Run("valid env overrides are applied", func(t *testing.T) {
		t.Setenv("TX_SLOW_LOG_AGE_THRESHOLD_SECONDS", "10")
		t.Setenv("TX_SLOW_LOG_CHANGE_THRESHOLD_SECONDS", "3")
		sl := newTxSlowLog(logger)
		assert.Equal(t, 10*time.Second, sl.ageThreshold)
		assert.Equal(t, 3*time.Second, sl.changeThreshold)
	})

	t.Run("invalid env keeps the defaults", func(t *testing.T) {
		t.Setenv("TX_SLOW_LOG_AGE_THRESHOLD_SECONDS", "not-a-number")
		t.Setenv("TX_SLOW_LOG_CHANGE_THRESHOLD_SECONDS", "also-bad")
		sl := newTxSlowLog(logger)
		assert.Equal(t, 5*time.Second, sl.ageThreshold)
		assert.Equal(t, 1*time.Second, sl.changeThreshold)
	})
}
