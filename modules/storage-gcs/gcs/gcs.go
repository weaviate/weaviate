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

package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"
	GOOGLE_CLOUD_PROJECT           = "GOOGLE_CLOUD_PROJECT"
	GCLOUD_PROJECT                 = "GCLOUD_PROJECT"
	GCP_PROJECT                    = "GCP_PROJECT"
)

type gcs struct {
	client    *storage.Client
	config    Config
	projectID string
}

func New(ctx context.Context, config Config) (*gcs, error) {
	options := []option.ClientOption{}
	if len(os.Getenv(GOOGLE_APPLICATION_CREDENTIALS)) > 0 {
		scopes := []string{
			"https://www.googleapis.com/auth/devstorage.read_write",
		}
		creds, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return nil, errors.Wrap(err, "find default credentials")
		}
		options = append(options, option.WithCredentials(creds))
	} else {
		options = append(options, option.WithoutAuthentication())
	}
	projectID := os.Getenv(GOOGLE_CLOUD_PROJECT)
	if len(projectID) == 0 {
		projectID = os.Getenv(GCLOUD_PROJECT)
		if len(projectID) == 0 {
			projectID = os.Getenv(GCP_PROJECT)
		}
	}
	client, err := storage.NewClient(ctx, options...)
	if err != nil {
		return nil, errors.Wrap(err, "create client")
	}
	return &gcs{client, config, projectID}, nil
}

func (g *gcs) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	bucketName := g.config.BucketName()
	projectID := g.projectID
	// create bucket
	bucketExists := false
	it := g.client.Buckets(ctx, projectID)
	for {
		bucketAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Wrap(err, "list buckets")
		}
		if bucketAttrs.Name == bucketName {
			bucketExists = true
			break
		}
	}
	if !bucketExists {
		err := g.client.Bucket(bucketName).Create(ctx, projectID, nil)
		if err != nil {
			errors.Wrap(err, "create bucket")
		}
	}
	bucketHandle := g.client.Bucket(bucketName)
	// save files
	snapshotID := snapshot.ID
	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "store snapshot aborted")
		}
		objectName := fmt.Sprintf("%s/%s", snapshotID, srcRelPath)
		filePath := fmt.Sprintf("%s/%s", snapshot.BasePath, srcRelPath)
		content, err := os.ReadFile(filePath)
		if err != nil {
			return errors.Wrapf(err, "read file: %v", filePath)
		}
		if err := g.putFile(ctx, bucketHandle, snapshotID, objectName, content); err != nil {
			return errors.Wrap(err, "put file")
		}
	}
	// save meta
	content, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Wrapf(err, "marshal meta")
	}
	objectName := fmt.Sprintf("%s/snapshot.json", snapshotID)
	if err := g.putFile(ctx, bucketHandle, snapshotID, objectName, content); err != nil {
		return errors.Wrap(err, "put file")
	}
	return nil
}

func (g *gcs) putFile(ctx context.Context, bucket *storage.BucketHandle,
	snapshotID, objectName string, content []byte,
) error {
	obj := bucket.Object(objectName)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/octet-stream"
	writer.Metadata = map[string]string{
		"snapshot-id": snapshotID,
	}
	if _, err := writer.Write(content); err != nil {
		return errors.Wrapf(err, "write file: %v", objectName)
	}
	if err := writer.Close(); err != nil {
		return errors.Wrapf(err, "close writer for file: %v", objectName)
	}
	return nil
}

func (g *gcs) RestoreSnapshot(ctx context.Context, snapshotId string) error {
	panic("not implemented")
}
