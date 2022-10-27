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

package modstggcs

import (
	"context"
	"io"
	"os"
	"path"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type gcsClient struct {
	client    *storage.Client
	config    clientConfig
	projectID string
	dataPath  string
}

func newClient(ctx context.Context, config *clientConfig, dataPath string) (*gcsClient, error) {
	options := []option.ClientOption{}
	if len(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")) > 0 {
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
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if len(projectID) == 0 {
		projectID = os.Getenv("GCLOUD_PROJECT")
		if len(projectID) == 0 {
			projectID = os.Getenv("GCP_PROJECT")
		}
	}
	client, err := storage.NewClient(ctx, options...)
	if err != nil {
		return nil, errors.Wrap(err, "create client")
	}
	return &gcsClient{client, *config, projectID, dataPath}, nil
}

func (g *gcsClient) getObject(ctx context.Context, bucket *storage.BucketHandle,
	backupID, objectName string,
) ([]byte, error) {
	// Create bucket reader
	obj := bucket.Object(objectName)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, err
		}
		return nil, errors.Wrapf(err, "new reader: %v", objectName)
	}
	// Read file contents
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read object: %v", objectName)
	}

	metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(len(content)))
	}
	return content, nil
}

func (g *gcsClient) HomeDir(backupID string) string {
	return "gs://" + path.Join(g.config.Bucket,
		g.makeObjectName(backupID))
}

func (g *gcsClient) findBucket(ctx context.Context) (*storage.BucketHandle, error) {
	bucket := g.client.Bucket(g.config.Bucket)

	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, err
	}

	return bucket, nil
}

func (g *gcsClient) makeObjectName(parts ...string) string {
	base := path.Join(parts...)
	return path.Join(g.config.BackupPath, base)
}

func (g *gcsClient) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	objectName := g.makeObjectName(backupID, key)

	if err := ctx.Err(); err != nil {
		return nil, backup.NewErrContextExpired(errors.Wrapf(err, "get object '%s'", objectName))
	}

	bucket, err := g.findBucket(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrBucketNotExist) {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object '%s'", objectName))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object '%s'", objectName))
	}

	contents, err := g.getObject(ctx, bucket, backupID, objectName)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object '%s'", objectName))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object '%s'", objectName))
	}

	return contents, nil
}

func (g *gcsClient) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	srcPath = path.Join(g.dataPath, srcPath)
	contents, err := os.ReadFile(srcPath)
	if err != nil {
		return errors.Wrapf(err, "read file '%s'", srcPath)
	}

	return g.PutObject(ctx, backupID, key, contents)
}

func (g *gcsClient) PutObject(ctx context.Context, backupID, key string, byes []byte) error {
	bucket, err := g.findBucket(ctx)
	if err != nil {
		return errors.Wrap(err, "find bucket")
	}

	objectName := g.makeObjectName(backupID, key)
	obj := bucket.Object(objectName)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/octet-stream"
	writer.Metadata = map[string]string{
		"backup-id": backupID,
	}
	if _, err := writer.Write(byes); err != nil {
		return errors.Wrapf(err, "write file: %v", objectName)
	}
	if err := writer.Close(); err != nil {
		return errors.Wrapf(err, "close writer for file: %v", objectName)
	}

	metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.GetMetricWithLabelValues("backup-gcs", "class")
	if err == nil {
		metric.Add(float64(len(byes)))
	}

	return nil
}

func (g *gcsClient) Initialize(ctx context.Context, backupID string) error {
	key := "access-check"

	if err := g.PutObject(ctx, backupID, key, []byte("")); err != nil {
		return errors.Wrap(err, "failed to access-check gcs backup module")
	}

	bucket, err := g.findBucket(ctx)
	if err != nil {
		return errors.Wrap(err, "find bucket")
	}

	objectName := g.makeObjectName(backupID, key)
	if err := bucket.Object(objectName).Delete(ctx); err != nil {
		return errors.Wrap(err, "failed to remove access-check gcs backup module")
	}

	return nil
}

func (g *gcsClient) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	obj, err := g.GetObject(ctx, backupID, key)
	if err != nil {
		return errors.Wrapf(err, "get object '%s'", key)
	}

	dir := path.Dir(destPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "make dir '%s'", dir)
	}

	if err := os.WriteFile(destPath, obj, os.ModePerm); err != nil {
		return errors.Wrapf(err, "write file '%s'", destPath)
	}

	return nil
}

func (g *gcsClient) SourceDataPath() string {
	return g.dataPath
}
