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
	"io"
	"os"
	"path"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"golang.org/x/oauth2/google"
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
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, snapshots.ErrNotFound{}
		}
		return nil, errors.Wrapf(err, "new reader: %v", objectName)
	}
	// Read file contents
	content, err := io.ReadAll(reader)
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

	destDir := path.Dir(targetPath)
	if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
		return errors.Wrapf(err,
			"save file: failed to make destination dir for file '%s'", targetPath)
	}

	// Write it to disk
	if err := os.WriteFile(targetPath, content, 0o644); err != nil {
		return errors.Wrapf(err, "write file: %v", targetPath)
	}
	return nil
}

func (g *gcs) RestoreSnapshot(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	timer := prometheus.NewTimer(monitoring.GetMetrics().SnapshotRestoreFromStorageDurations.WithLabelValues("gcs", className))
	defer timer.ObserveDuration()
	bucket, err := g.findBucket(ctx)
	if err != nil || bucket == nil {
		return nil, errors.Wrap(err, "snapshot bucket does not exist")
	}

	// Download metadata for snapshot
	objectName := g.makeObjectName(className, snapshotID, "snapshot.json")
	reader, err := bucket.Object(objectName).NewReader(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "new reader: %v", objectName)
	}

	// Fetch content
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read object: %v", objectName)
	}

	// Unmarshal content into snapshot struct
	var snapshot snapshots.Snapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		return nil, errors.Wrapf(err, "unmarshal snapshot: %v", objectName)
	}

	// download files listed in snapshot
	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return nil, errors.Wrapf(err, "store snapshot aborted")
		}
		objectName := g.makeObjectName(className, snapshotID, srcRelPath)
		filePath := makeFilePath(g.dataPath, srcRelPath)
		if err := g.saveFile(ctx, bucket, snapshotID, objectName, filePath); err != nil {
			return nil, errors.Wrap(err, "put file")
		}

		// Get size of file
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return nil, errors.Errorf("Unable to get size of file %v", filePath)
		}
		monitoring.GetMetrics().SnapshotRestoreDataTransferred.WithLabelValues("gcs", className).Add(float64(fileInfo.Size()))
	}
	return &snapshot, nil
}

func (g *gcs) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	timer := prometheus.NewTimer(monitoring.GetMetrics().SnapshotStoreDurations.WithLabelValues("gcs", snapshot.ClassName))
	defer timer.ObserveDuration()
	bucket, err := g.findBucket(ctx)
	if err != nil {
		return err
	}

	if bucket == nil {
		return snapshots.NewErrNotFound(storage.ErrBucketNotExist)
	}

	// save files
	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "store snapshot aborted")
		}
		objectName := g.makeObjectName(snapshot.ClassName, snapshot.ID, srcRelPath)
		filePath := makeFilePath(g.dataPath, srcRelPath)
		content, err := os.ReadFile(filePath)
		if err != nil {
			return errors.Wrapf(err, "read file: %v", filePath)
		}

		if err := g.putFile(ctx, bucket, snapshot.ID, objectName, content); err != nil {
			return errors.Wrap(err, "put file")
		}

		monitoring.GetMetrics().SnapshotStoreDataTransferred.WithLabelValues("gcs", snapshot.ClassName).Add(float64(len(content)))
	}

	if err := g.putMeta(ctx, bucket, snapshot); err != nil {
		return errors.Wrap(err, "store snapshot")
	}

	return nil
}

func (g *gcs) putMeta(ctx context.Context, bucket *storage.BucketHandle, snapshot *snapshots.Snapshot) error {
	content, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Wrap(err, "failed to marshal meta")
	}
	objectName := g.makeObjectName(snapshot.ClassName, snapshot.ID, "snapshot.json")
	if err := g.putFile(ctx, bucket, snapshot.ID, objectName, content); err != nil {
		return errors.Wrap(err, "failed to store meta")
	}
	return nil
}

func (g *gcs) GetMeta(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	bucket, err := g.findBucket(ctx)
	if err != nil {
		return nil, err
	}

	if bucket == nil {
		return nil, snapshots.ErrNotFound{}
	}

	objectName := g.makeObjectName(className, snapshotID, "snapshot.json")
	contents, err := g.getObject(ctx, bucket, snapshotID, objectName)
	if err != nil {
		return nil, err
	}

	var snapshot snapshots.Snapshot
	err = json.Unmarshal(contents, &snapshot)
	if err != nil {
		return nil, errors.Wrap(err, "get snapshot status")
	}

	return &snapshot, nil
}

func (g *gcs) SetMetaError(ctx context.Context, className, snapshotID string, snapErr error) error {
	bucket, err := g.findBucket(ctx)
	if err != nil {
		return errors.Wrap(err, "set snapshot error")
	}

	objectName := g.makeObjectName(className, snapshotID, "snapshot.json")
	contents, err := g.getObject(ctx, bucket, snapshotID, objectName)
	if err != nil {
		return errors.Wrap(err, "set snapshot status")
	}

	var snapshot snapshots.Snapshot
	err = json.Unmarshal(contents, &snapshot)
	if err != nil {
		return errors.Wrap(err, "set meta error")
	}

	snapshot.Status = string(snapshots.CreateFailed)
	snapshot.Error = snapErr.Error()

	if err := g.putMeta(ctx, bucket, &snapshot); err != nil {
		return errors.Wrap(err, "set meta error")
	}

	return nil
}

func (g *gcs) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	bucket, err := g.findBucket(ctx)
	if err != nil {
		return errors.Wrap(err, "set meta status")
	}

	objectName := g.makeObjectName(className, snapshotID, "snapshot.json")
	contents, err := g.getObject(ctx, bucket, snapshotID, objectName)
	if err != nil {
		return errors.Wrap(err, "set meta status")
	}

	var snapshot snapshots.Snapshot
	err = json.Unmarshal(contents, &snapshot)
	if err != nil {
		return errors.Wrap(err, "set meta status")
	}

	if status == string(snapshots.CreateSuccess) {
		snapshot.CompletedAt = time.Now()
	}

	snapshot.Status = status

	if err := g.putMeta(ctx, bucket, &snapshot); err != nil {
		return errors.Wrap(err, "set meta status")
	}

	return nil
}

func (g *gcs) DestinationPath(className, snapshotID string) string {
	return "gs://" + path.Join(g.config.BucketName(),
		g.makeObjectName(className, snapshotID, "snapshot.json"))
}

func (g *gcs) InitSnapshot(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	bucket, err := g.findBucket(ctx)
	if err != nil && !errors.Is(err, snapshots.ErrNotFound{}) {
		return nil, errors.Wrap(err, "init snapshot")
	}

	snapshot := snapshots.New(className, snapshotID, time.Now())
	snapshot.Status = string(snapshots.CreateStarted)
	b, err := json.Marshal(&snapshot)
	if err != nil {
		return nil, errors.Wrap(err, "init snapshot")
	}

	objectName := g.makeObjectName(className, snapshotID, "snapshot.json")

	if err := g.putFile(ctx, bucket, snapshot.ID, objectName, b); err != nil {
		return nil, errors.Wrap(err, "init snapshot")
	}

	return snapshot, nil
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

func (g *gcs) findBucket(ctx context.Context) (*storage.BucketHandle, error) {
	bucket := g.client.Bucket(g.config.BucketName())

	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, err
	}

	return bucket, nil
}

func (g *gcs) makeObjectName(parts ...string) string {
	base := path.Join(parts...)
	return path.Join(g.config.SnapshotRoot(), base)
}

func makeFilePath(parts ...string) string {
	return path.Join(parts...)
}
