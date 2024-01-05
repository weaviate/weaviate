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

package modstggcs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/monitoring"
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
	useAuth := strings.ToLower(os.Getenv("BACKUP_GCS_USE_AUTH")) != "false"
	if useAuth {
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

	client.SetRetry(storage.WithBackoff(gax.Backoff{
		Initial:    2 * time.Second, // Note: the client uses a jitter internally
		Max:        60 * time.Second,
		Multiplier: 3,
	}),
		storage.WithPolicy(storage.RetryAlways),
	)
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

// PutFile creates an object with contents from file at filePath.
func (g *gcsClient) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	bucket, err := g.findBucket(ctx)
	if err != nil {
		return fmt.Errorf("find bucket: %w", err)
	}

	// open source file
	filePath := path.Join(g.dataPath, srcPath)
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("os.open %q: %w", filePath, err)
	}
	defer file.Close()

	// create a new writer
	object := g.makeObjectName(backupID, key)
	writer := bucket.Object(object).NewWriter(ctx)
	writer.ContentType = "application/octet-stream"
	writer.Metadata = map[string]string{"backup-id": backupID}

	// if we return early make sure writer is closed
	closeWriter := true
	defer func() {
		if closeWriter {
			writer.Close()
		}
	}()

	nBytes, err := io.Copy(writer, file)
	if err != nil {
		return fmt.Errorf("io.copy %q %q: %w", object, filePath, err)
	}
	closeWriter = false
	if err := writer.Close(); err != nil {
		return fmt.Errorf("writer.close %q: %w", filePath, err)
	}
	metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.GetMetricWithLabelValues("backup-gcs", "class")
	if err == nil {
		metric.Add(float64(nBytes))
	}
	return nil
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

// WriteToFile downloads an object and store its content in destPath
// The file destPath will be created if it doesn't exit
func (g *gcsClient) WriteToFile(ctx context.Context, backupID, key, destPath string) (err error) {
	bucket, err := g.findBucket(ctx)
	if err != nil {
		return fmt.Errorf("find bucket: %w", err)
	}

	// validate destination path
	if st, err := os.Stat(destPath); err == nil {
		if st.IsDir() {
			return fmt.Errorf("file is a directory")
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	// create empty file
	dir := path.Dir(destPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("os.mkdir %q: %w", dir, err)
	}
	file, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("os.create %q: %w", destPath, err)
	}

	// make sure to close and delete in case we return early
	closeAndRemove := true
	defer func() {
		if closeAndRemove {
			file.Close()
			os.Remove(destPath)
		}
	}()

	// create reader
	object := g.makeObjectName(backupID, key)
	rc, err := bucket.Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("find object %q: %w", object, err)
	}
	defer rc.Close()

	// transfer content to the file
	if _, err := io.Copy(file, rc); err != nil {
		return fmt.Errorf("io.Copy:%q %q: %w", destPath, object, err)
	}
	closeAndRemove = false
	if err = file.Close(); err != nil {
		return fmt.Errorf("f.Close %q: %w", destPath, err)
	}

	return nil
}

func (g *gcsClient) Write(ctx context.Context, backupID, key string, r io.ReadCloser) (int64, error) {
	defer r.Close()

	bucket, err := g.findBucket(ctx)
	if err != nil {
		return 0, fmt.Errorf("find bucket: %w", err)
	}

	// create a new writer
	path := g.makeObjectName(backupID, key)
	writer := bucket.Object(path).NewWriter(ctx)
	writer.ContentType = "application/octet-stream"
	writer.Metadata = map[string]string{"backup-id": backupID}

	// if we return early make sure writer is closed
	closeWriter := true
	defer func() {
		if closeWriter {
			writer.Close()
		}
	}()

	// copy
	written, err := io.Copy(writer, r)
	if err != nil {
		return 0, fmt.Errorf("io.copy %q: %w", path, err)
	}
	closeWriter = false
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("writer.close %q: %w", path, err)
	}
	if metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.
		GetMetricWithLabelValues(Name, "class"); err == nil {
		metric.Add(float64(written))
	}
	return written, nil
}

func (g *gcsClient) Read(ctx context.Context, backupID, key string, w io.WriteCloser) (int64, error) {
	defer w.Close()

	bucket, err := g.findBucket(ctx)
	if err != nil {
		err = fmt.Errorf("find bucket: %w", err)
		if errors.Is(err, storage.ErrObjectNotExist) {
			err = backup.NewErrNotFound(err)
		}
		return 0, err
	}

	// create reader
	path := g.makeObjectName(backupID, key)
	rc, err := bucket.Object(path).NewReader(ctx)
	if err != nil {
		err = fmt.Errorf("find object %s: %v", path, err)
		if errors.Is(err, storage.ErrObjectNotExist) {
			err = backup.NewErrNotFound(err)
		}
		return 0, err
	}
	defer rc.Close()

	// copy
	read, err := io.Copy(w, rc)
	if err != nil {
		return read, fmt.Errorf("io.copy %q: %w", path, err)
	}

	if metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.
		GetMetricWithLabelValues(Name, "class"); err == nil {
		metric.Add(float64(float64(read)))
	}

	return read, nil
}

func (g *gcsClient) SourceDataPath() string {
	return g.dataPath
}
