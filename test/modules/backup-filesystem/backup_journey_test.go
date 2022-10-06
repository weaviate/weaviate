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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/test/docker"
	"github.com/semi-technologies/weaviate/test/helper/journey"
	"github.com/stretchr/testify/require"
)

func Test_BackupJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithBackendFilesystem().
		WithText2VecContextionary().
		WithWeaviate().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminte test containers: %s", err.Error())
		}
	}()

	t.Run("backup-filesystem", func(t *testing.T) {
		journey.BackupJourneyTests(t, compose.GetWeaviate().URI(),
			"filesystem", "FileSystemClass", "filesystem-backup-1")
	})
}
