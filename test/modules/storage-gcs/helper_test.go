package test

import (
	"context"
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func createBucket(ctx context.Context, t *testing.T, projectID, bucketName string) {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.Nil(t, err)

	err = client.Bucket(bucketName).Create(ctx, projectID, nil)
	gcsErr, ok := err.(*googleapi.Error)
	if ok {
		// the bucket persists from the previous test.
		// if the bucket already exists, we can proceed
		if gcsErr.Code == http.StatusConflict {
			return
		}
	}
	require.Nil(t, err)
}
