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

package test

import (
	"context"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

func createBucket(ctx context.Context, t *testing.T, endpoint, region, bucketName string) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Region: region,
		Secure: false,
	})
	require.Nil(t, err)

	err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	minioErr, ok := err.(minio.ErrorResponse)
	if ok {
		// the bucket persists from a previous test.
		// if the bucket already exists, we can proceed
		if minioErr.Code == "BucketAlreadyOwnedByYou" {
			return
		}
	}
	require.Nil(t, err)
}
