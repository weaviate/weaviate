//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardStatusValidation(t *testing.T) {
	t.Run("with valid status", func(t *testing.T) {
		status := []string{"READONLY", "READY"}

		for _, s := range status {
			isValid := isValidShardStatus(s)
			require.Truef(t, isValid, "status %s is valid and should be true", s)
		}
	})

	t.Run("with invalid status", func(t *testing.T) {
		status := []string{
			"READ_ONLY",
			"readonly",
			"ready",
			"WRITEONLY",
			"",
		}

		for _, s := range status {
			isValid := isValidShardStatus(s)
			require.False(t, isValid)
		}
	})
}

func TestShardsStatus_Get_Update(t *testing.T) {
	s := &Shard{
		name:       "testshard",
		statusLock: &sync.Mutex{},
		status:     ShardStatusReady,
	}

	t.Run("get status", func(t *testing.T) {
		status := s.getStatus()
		require.Equal(t, ShardStatusReady, status)
	})

	t.Run("update status success", func(t *testing.T) {
		err := s.updateStatus(ShardStatusReadOnly)
		require.Nil(t, err)

		status := s.getStatus()
		require.Equal(t, ShardStatusReadOnly, status)
	})

	t.Run("update status failure", func(t *testing.T) {
		err := s.updateStatus("invalid_status")
		require.EqualError(t, err, "'invalid_status' is not a valid shard status")
	})
}
