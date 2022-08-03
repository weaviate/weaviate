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

	s3 "github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_S3Storage_StoreSnapshot(t *testing.T) {
	testdataMainDir := "./testData"

	t.Run("store snapshot in s3", func(t *testing.T) {
		testDir := makeTestDir(t, testdataMainDir)
		defer removeDir(t, testdataMainDir)

		snapshot := createSnapshotInstance(t, testDir)
		ctxSnapshot := context.Background()

		logger, _ := test.NewNullLogger()

		os.Setenv("AWS_REGION", "eu-west-1")
		os.Setenv("AWS_ACCESS_KEY", "aws_access_key")
		os.Setenv("AWS_SECRET_KEY", "aws_secret_key")

		endpoint := os.Getenv(minioEndpoint)
		s3Config := s3.NewConfig(endpoint, "bucket", false)
		s3, err := s3.New(s3Config, logger)
		require.Nil(t, err)

		err = s3.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)
	})
}
