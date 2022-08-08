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
	"io"
	"io/ioutil"
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
	dataPath  string
}

func New(ctx context.Context, config Config, dataPath string) (*gcs, error) {
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
	return &gcs{client, config, projectID, dataPath}, nil
}

func (g *gcs) getObject(ctx context.Context, bucket *storage.BucketHandle,
	snapshotID, objectName string,
) ([]byte, error) {
	// Create bucket reader
	obj := bucket.Object(objectName)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "new reader: %v", objectName)
	}
	// Read file contents
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read object: %v", objectName)
	}
	return content, nil
}

func (g *gcs) saveFile(ctx context.Context, bucket *storage.BucketHandle,
	snapshotID, objectName, targetPath string,
) error {
	// Get the file from the bucket
	content, err := g.getObject(ctx, bucket, snapshotID, objectName)
	if err != nil {
		return errors.Wrapf(err, "get object: %v", objectName)
	}

	// Write it to disk
	if err := ioutil.WriteFile(targetPath, content, 0o644); err != nil {
		return errors.Wrapf(err, "write file: %v", targetPath)
	}
	return nil
}

func (g *gcs) RestoreSnapshot(ctx context.Context, snapshotID string) error {
	bucketName := g.config.BucketName()
	projectID := g.projectID
	// Find bucket
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

	// Bucket must exist to restore from it
	if !bucketExists {
		errors.New("snapshot bucket does not exist")
	}
	bucketHandle := g.client.Bucket(bucketName)

	// Download metadata for snapshot
	objectName := fmt.Sprintf("%s/snapshot.json", snapshotID)
	reader, err := bucketHandle.Object(objectName).NewReader(ctx)
	if err != nil {
		return errors.Wrapf(err, "new reader: %v", objectName)
	}

	// Fetch content
	content, err := io.ReadAll(reader)
	if err != nil {
		return errors.Wrapf(err, "read object: %v", objectName)
	}

	// Unmarshal content into snapshot struct
	var snapshot snapshots.Snapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		return errors.Wrapf(err, "unmarshal snapshot: %v", objectName)
	}

	// download files listed in snapshot
	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "store snapshot aborted")
		}
		objectName := fmt.Sprintf("%s/%s", snapshotID, srcRelPath)
		filePath := fmt.Sprintf("%s/%s", snapshot.BasePath, srcRelPath)
		if err := g.saveFile(ctx, bucketHandle, snapshotID, objectName, filePath); err != nil {
			return errors.Wrap(err, "put file")
		}
	}
	return nil
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
		filePath := fmt.Sprintf("%s/%s", g.dataPath, srcRelPath)
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
