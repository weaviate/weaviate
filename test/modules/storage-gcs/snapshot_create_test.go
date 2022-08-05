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
	"os"
	"testing"

	"github.com/semi-technologies/weaviate/modules/storage-gcs/gcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GCSStorage_StoreSnapshot(t *testing.T) {
	testdataMainDir := "./testData"

	t.Run("store snapshot in gcs", func(t *testing.T) {
		testDir := makeTestDir(t, testdataMainDir)
		defer removeDir(t, testdataMainDir)

		snapshot := createSnapshotInstance(t, testDir)
		ctxSnapshot := context.Background()

		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
		os.Setenv("GOOGLE_CLOUD_PROJECT", "project-id")
		os.Setenv("STORAGE_EMULATOR_HOST", os.Getenv(gcsEndpoint))

		gcsConfig := gcs.NewConfig("")
		gcs, err := gcs.New(context.Background(), gcsConfig)
		require.Nil(t, err)

		err = gcs.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)
	})
}
