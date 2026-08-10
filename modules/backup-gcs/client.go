//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modstggcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/weaviate/weaviate/entities/backup"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	ucfg "github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/gcpcommon"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type gcsClient struct {
	client    *storage.Client
	config    clientConfig
	projectID string
	dataPath  string
	logger    logrus.FieldLogger
	nodeID    string        // hostname, used to make access-check paths unique across nodes
	counter   atomic.Uint64 // monotonic counter for unique access-check paths within a node
}

const (
	// chunkRetryDeadline budgets all attempts at one chunk, backoff pauses
	// included. The SDK default of 32s expires partway through the ladder
	// newClient configures (2s, 6s, 18s, 54s), so its 60s ceiling never applies.
	chunkRetryDeadline = 5 * time.Minute

	// chunkTransferTimeout bounds one chunk request. chunkRetryDeadline does not:
	// it is only checked between attempts, so without this a stalled request
	// hangs until the caller's context expires, and backup uploads run under a
	// 24h timeout. It is a hard timeout on the request rather than a stall
	// detector, so it must stay well above the time a full chunk takes on a slow
	// link, and well below chunkRetryDeadline to leave room for retries. Raising
	// the writer's ChunkSize means revisiting both. HTTP only; the gRPC writer
	// ignores it.
	chunkTransferTimeout = time.Minute
)

// newChunkWriter returns a writer for one backup chunk, tuned for the
// multi-chunk uploads Write streams.
func newChunkWriter(ctx context.Context, obj *storage.ObjectHandle, backupID string) *storage.Writer {
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/octet-stream"
	writer.Metadata = map[string]string{"backup-id": backupID}
	writer.ChunkRetryDeadline = chunkRetryDeadline
	writer.ChunkTransferTimeout = chunkTransferTimeout
	return writer
}

func storageOptions(ctx context.Context, logger logrus.FieldLogger, transport ucfg.BackupGCS) ([]option.ClientOption, error) {
	opts := []option.ClientOption{}
	useAuth := strings.ToLower(os.Getenv("BACKUP_GCS_USE_AUTH")) != "false"
	backupGCSAuthProxyEndpoint := os.Getenv("BACKUP_GCS_AUTH_PROXY_ENDPOINT")

	if useAuth {
		scopes := []string{
			"https://www.googleapis.com/auth/devstorage.read_write",
		}
		creds, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return nil, errors.Wrap(err, "find default credentials")
		}
		opts = append(opts, option.WithCredentials(creds))
	} else if backupGCSAuthProxyEndpoint != "" {
		logger.Info("backup-gcs: using auth broker for GCS credentials")
		opts = append(
			opts,
			option.WithTokenSource(
				oauth2.ReuseTokenSource(nil, gcpcommon.NewAuthBrokerTokenSource(backupGCSAuthProxyEndpoint)),
			),
		)
	} else {
		opts = append(opts, option.WithoutAuthentication())
	}

	if transport.UseGRPC {
		// The SDK exports gRPC client metrics to Cloud Monitoring by default. That
		// needs monitoring.timeSeries.create and reports its own failures through
		// the standard library, bypassing our logger.
		opts = append(opts,
			option.WithGRPCConnectionPool(transport.GRPCConnPool),
			storage.WithDisabledClientMetrics(),
		)
	}

	return opts, nil
}

func projectID() string {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if len(projectID) == 0 {
		projectID = os.Getenv("GCLOUD_PROJECT")
		if len(projectID) == 0 {
			projectID = os.Getenv("GCP_PROJECT")
		}
	}

	return projectID
}

func newClient(ctx context.Context, config *clientConfig, dataPath string, logger logrus.FieldLogger) (*gcsClient, error) {
	opts, err := storageOptions(ctx, logger, config.Transport)
	if err != nil {
		return nil, err
	}

	var client *storage.Client
	if config.Transport.UseGRPC {
		logger.Infof("backup-gcs: using gRPC transport with a connection pool of %d per client", config.Transport.GRPCConnPool)
		client, err = storage.NewGRPCClient(ctx, opts...)
	} else {
		client, err = storage.NewClient(ctx, opts...)
	}
	if err != nil {
		return nil, errors.Wrap(err, "create client")
	}

	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Initial:    2 * time.Second, // Note: the client uses a jitter internally
			Max:        60 * time.Second,
			Multiplier: 3,
		}),
		storage.WithPolicy(storage.RetryAlways),
		storage.WithErrorFunc(gcpcommon.RetryErrorFunc),
	)
	nodeID, err := os.Hostname()
	if err != nil {
		nodeID = strconv.Itoa(os.Getpid())
	}
	return &gcsClient{client: client, config: *config, projectID: projectID(), dataPath: dataPath, logger: logger, nodeID: nodeID}, nil
}

func (g *gcsClient) getObject(ctx context.Context, bucket *storage.BucketHandle,
	objectName string,
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
	defer reader.Close()

	// Read file contents using io.Copy for better memory management
	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read object: %v", objectName)
	}

	content := buf.Bytes()
	metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(len(content)))
	}
	return content, nil
}

func (g *gcsClient) HomeDir(backupID, overrideBucket, overridePath string) string {
	if overridePath == "" && overrideBucket == "" {
		return "gs://" + path.Join(g.config.Bucket,
			g.makeObjectName("", []string{backupID}))
	} else {
		return "gs://" + path.Join(overrideBucket,
			g.makeObjectName(overridePath, []string{backupID}))
	}
}

func (g *gcsClient) AllBackups(ctx context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	bucket, err := g.findBucket(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("find bucket: %w", err)
	}

	// Use delimiter listing to get one-level-deep prefixes (one per backup ID)
	// instead of scanning all objects
	prefix := g.config.BackupPath
	if prefix != "" {
		prefix += "/"
	}
	iter := bucket.Objects(ctx, &storage.Query{Prefix: prefix, Delimiter: "/"})

	var keys []string
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		next, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("get next object: %w", err)
		}

		if next.Prefix != "" {
			keys = append(keys, next.Prefix+ubak.GlobalBackupFile)
		}
	}

	return ubak.FetchBackupDescriptors(ctx, g.logger, keys, func(ctx context.Context, key string) ([]byte, error) {
		data, err := g.getObject(ctx, bucket, key)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object %s", key))
			}
			return nil, backup.NewErrInternal(errors.Wrapf(err, "get object %s", key))
		}

		return data, nil
	})
}

func (g *gcsClient) resolveBucketName(bucketOverride string) (string, error) {
	b := g.config.Bucket
	if bucketOverride != "" {
		b = bucketOverride
	}
	if b == "" {
		return "", fmt.Errorf("bucket must not be empty")
	}
	return b, nil
}

func (g *gcsClient) findBucket(ctx context.Context, bucketOverride string) (*storage.BucketHandle, error) {
	b, err := g.resolveBucketName(bucketOverride)
	if err != nil {
		return nil, err
	}
	bucket := g.client.Bucket(b)

	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, fmt.Errorf("find bucket: %w", err)
	}

	return bucket, nil
}

func (g *gcsClient) makeObjectName(overridePath string, parts []string) string {
	if overridePath != "" {
		base := path.Join(parts...)
		return path.Join(overridePath, base)
	} else {
		base := path.Join(parts...)
		return path.Join(g.config.BackupPath, base)
	}
}

func (g *gcsClient) GetObject(ctx context.Context, backupID, key, overrideBucket, overridePath string) ([]byte, error) {
	objectName := g.makeObjectName(overridePath, []string{backupID, key})

	if err := ctx.Err(); err != nil {
		return nil, backup.NewErrContextExpired(errors.Wrapf(err, "get object %s", objectName))
	}

	bucket, err := g.findBucket(ctx, overrideBucket)
	if err != nil {
		if errors.Is(err, storage.ErrBucketNotExist) {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object %s", objectName))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object %s", objectName))
	}

	contents, err := g.getObject(ctx, bucket, objectName)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object %s", objectName))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object %s", objectName))
	}

	return contents, nil
}

func (g *gcsClient) PutObject(ctx context.Context, backupID, key, overrideBucket, overridePath string, byes []byte) error {
	bucket, err := g.findBucket(ctx, overrideBucket)
	if err != nil {
		return errors.Wrap(err, "find bucket")
	}

	objectName := g.makeObjectName(overridePath, []string{backupID, key})
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

func (g *gcsClient) Initialize(ctx context.Context, backupID, overrideBucket, overridePath string) error {
	if _, err := g.resolveBucketName(overrideBucket); err != nil {
		return err
	}

	if g.config.SkipAccessCheck {
		return nil
	}

	// Each call gets a unique access-check file so concurrent Initialize calls
	// from different nodes (or the same node) never interfere with each other.
	seq := g.counter.Add(1)
	key := "access-check-" + g.nodeID + "-" + strconv.FormatUint(seq, 10)

	if err := g.PutObject(ctx, backupID, key, overrideBucket, overridePath, []byte("")); err != nil {
		return errors.Wrapf(err, "failed to access-check gcs backup module %v %v %v %v", overrideBucket, overridePath, backupID, key)
	}

	bucket, err := g.findBucket(ctx, overrideBucket)
	if err != nil {
		return errors.Wrap(err, "find bucket")
	}

	objectName := g.makeObjectName(overridePath, []string{backupID, key})
	if err := bucket.Object(objectName).Delete(ctx); err != nil {
		return errors.Wrapf(err, "failed to remove access-check gcs backup module %v", objectName)
	}

	return nil
}

func (g *gcsClient) Write(ctx context.Context, backupID, key, overrideBucket, overridePath string, r backup.ReadCloserWithError) (written int64, err error) {
	// Close the reader when done. Use CloseWithError to signal any error to the
	// producer so it sees the actual error instead of "closed pipe".
	defer func() {
		r.CloseWithError(err)
	}()

	bucket, err := g.findBucket(ctx, overrideBucket)
	if err != nil {
		return 0, fmt.Errorf("write: find bucket: %w", err)
	}

	// create a new writer
	objectPath := g.makeObjectName(overridePath, []string{backupID, key})
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := newChunkWriter(writeCtx, bucket.Object(objectPath), backupID)

	// copy
	written, err = io.Copy(writer, r)
	if err != nil {
		// Cancelling abandons the upload; closing the writer instead would finalize it,
		// storing the bytes copied so far as a complete object. The close afterwards
		// ends the writer's trace span and cannot revive the upload, whose next request
		// would need the context just cancelled.
		cancel()
		writer.Close()
		return written, fmt.Errorf("io.copy for gcs write %q: %w", objectPath, err)
	}

	if err := writer.Close(); err != nil {
		return written, fmt.Errorf("close writer for gcs write %q: %w", objectPath, err)
	}

	if metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.
		GetMetricWithLabelValues(Name, "class"); err == nil {
		metric.Add(float64(written))
	}
	return written, nil
}

func (g *gcsClient) Read(ctx context.Context, backupID, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error) {
	defer w.Close()

	bucket, err := g.findBucket(ctx, overrideBucket)
	if err != nil {
		err = fmt.Errorf("read: find bucket: %w", err)
		if errors.Is(err, storage.ErrBucketNotExist) {
			err = backup.NewErrNotFound(err)
		}
		return 0, err
	}

	// create reader
	path := g.makeObjectName(overridePath, []string{backupID, key})
	rc, err := bucket.Object(path).NewReader(ctx)
	if err != nil {
		err = fmt.Errorf("create reader %s: %w", path, err)
		if errors.Is(err, storage.ErrObjectNotExist) {
			err = backup.NewErrNotFound(err)
		}
		return 0, err
	}
	defer rc.Close()

	// copy
	read, err := io.Copy(w, rc)
	if err != nil {
		return read, fmt.Errorf("io.copy for read %q: %w", path, err)
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
