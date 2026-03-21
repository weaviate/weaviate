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

package cron

import (
	"context"
	"testing"

	gocron "github.com/netresearch/go-cron"
	"github.com/stretchr/testify/require"
)

func TestGoCronInit(t *testing.T) {
	t.Run("cron accepts different schedule formats", func(t *testing.T) {
		cr := initGoCron(context.Background(), gocron.DiscardLogger)

		t.Run("job with valid schudule is added", func(t *testing.T) {
			schedules := []string{
				"@every 1m",
				"0 16 * * *",
				"0 0 16 * * *",
				"0 */2 * * *",
				"1 0 */3 * * *",
				"30 14 25 12 * 2027",
				"0 30 14 25 12 * 2027",
				"0 30 14 25 12 *",
				"30 14 25 12 *",
			}

			for _, schedule := range schedules {
				t.Run(schedule, func(t *testing.T) {
					entryId, err := cr.AddFunc(schedule, func() {})

					require.NoError(t, err)
					require.NotZero(t, entryId)
				})
			}
		})

		t.Run("job with invalid schedule is not added", func(t *testing.T) {
			schedules := []string{
				"0 16 * *",
				"0 0 30 14 25 12 * 2027",
				"a b c d e",
			}

			for _, schedule := range schedules {
				t.Run(schedule, func(t *testing.T) {
					entryId, err := cr.AddFunc(schedule, func() {})

					require.Error(t, err)
					require.Zero(t, entryId)
				})
			}
		})
	})
}
