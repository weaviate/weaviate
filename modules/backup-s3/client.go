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

package modstgs3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	// source : https://github.com/minio/minio-go/blob/master/api-put-object-common.go#L69
	// minio has min part size of 16MB
	MINIO_MIN_PART_SIZE = 16 * 1024 * 1024
)

type s3Client struct {
	client   *minio.Client
	config   *clientConfig
	logger   logrus.FieldLogger
	dataPath string
	region   string
}

func newClient(config *clientConfig, logger logrus.FieldLogger, dataPath, bucket, path string) (*s3Client, error) {
	region := os.Getenv("AWS_REGION")
	if len(region) == 0 {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}

	var creds *credentials.Credentials
	if (os.Getenv("AWS_ACCESS_KEY_ID") != "" || os.Getenv("AWS_ACCESS_KEY") != "") &&
		(os.Getenv("AWS_SECRET_ACCESS_KEY") != "" || os.Getenv("AWS_SECRET_KEY") != "") {
		creds = credentials.NewEnvAWS()
	} else {
		creds = credentials.NewIAM("")
		// .Get() got deprecated with 7.0.83
		// and passing nil will use default context,
		if _, err := creds.GetWithContext(nil); err != nil {
			// can be anonymous access
			creds = credentials.NewEnvAWS()
		}
	}

	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  creds,
		Region: region,
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create client")
	}
	return &s3Client{client, config, logger, dataPath, region}, nil
}

func (s *s3Client) getClient(ctx context.Context) (*minio.Client, error) {
	xAwsAccessKey := modulecomponents.GetValueFromContext(ctx, "X-AWS-ACCESS-KEY")
	xAwsSecretKey := modulecomponents.GetValueFromContext(ctx, "X-AWS-SECRET-KEY")
	xAwsSessionToken := modulecomponents.GetValueFromContext(ctx, "X-AWS-SESSION-TOKEN")
	if xAwsAccessKey != "" && xAwsSecretKey != "" && xAwsSessionToken != "" {
		return minio.New(s.config.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(xAwsAccessKey, xAwsSecretKey, xAwsSessionToken),
			Region: s.region,
			Secure: s.config.UseSSL,
		})
	}
	return s.client, nil
}

func (s *s3Client) makeObjectName(parts ...string) string {
	base := path.Join(parts...)
	return path.Join(s.config.BackupPath, base)
}

func (s *s3Client) HomeDir(backupID, overrideBucket, overridePath string) string {
	remoteBucket := s.config.Bucket
	remotePath := s.config.BackupPath

	if overridePath != "" {
		remotePath = path.Join(overridePath)
	}

	if overrideBucket != "" {
		remoteBucket = overrideBucket
	}

	return "s3://" + path.Join(remoteBucket, remotePath, s.makeObjectName(backupID))
}

func (s *s3Client) AllBackups(ctx context.Context,
) ([]*backup.DistributedBackupDescriptor, error) {
	var meta []*backup.DistributedBackupDescriptor
	objectsInfo := s.client.ListObjects(ctx,
		s.config.Bucket,
		minio.ListObjectsOptions{
			Recursive: true,
			Prefix:    s.config.BackupPath,
		},
	)
	for info := range objectsInfo {
		if strings.Contains(info.Key, ubak.GlobalBackupFile) {
			obj, err := s.client.GetObject(ctx,
				s.config.Bucket, info.Key, minio.GetObjectOptions{})
			if err != nil {
				return nil, fmt.Errorf("get object %q: %w", info.Key, err)
			}
			contents, err := io.ReadAll(obj)
			if err != nil {
				return nil, fmt.Errorf("read object %q: %w", info.Key, err)
			}
			var desc backup.DistributedBackupDescriptor
			if err := json.Unmarshal(contents, &desc); err != nil {
				return nil, fmt.Errorf("unmarshal object %q: %w", info.Key, err)
			}
			meta = append(meta, &desc)
		}
	}
	return meta, nil
}

func (s *s3Client) GetObject(ctx context.Context, backupID, key, overrideBucket, overridePath string) ([]byte, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get object: failed to get client")
	}
	remotePath := s.makeObjectName(backupID, key)

	if overridePath != "" {
		remotePath = path.Join(overridePath, backupID, key)
	}

	bucket := s.config.Bucket
	if overrideBucket != "" {
		bucket = overrideBucket
	}

	if err := ctx.Err(); err != nil {
		return nil, backup.NewErrContextExpired(errors.Wrapf(err, "context expired in get object %s", remotePath))
	}

	obj, err := client.GetObject(ctx, bucket, remotePath, minio.GetObjectOptions{})
	if err != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object %s", remotePath))
	}

	contents, err := io.ReadAll(obj)
	if err != nil {
		var s3Err minio.ErrorResponse
		if errors.As(err, &s3Err) && s3Err.StatusCode == http.StatusNotFound {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object contents from %s:%s not found %s", bucket, remotePath, remotePath))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object contents from %s:%s %s", bucket, remotePath, remotePath))
	}

	metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(len(contents)))
	}

	return contents, nil
}

func (s *s3Client) PutObject(ctx context.Context, backupID, key, overrideBucket, overridePath string, byes []byte) error {
	client, err := s.getClient(ctx)
	if err != nil {
		return errors.Wrap(err, "put object: failed to get client")
	}

	remotePath := s.makeObjectName(backupID, key)
	opt := minio.PutObjectOptions{
		ContentType:    "application/octet-stream",
		PartSize:       MINIO_MIN_PART_SIZE,
		SendContentMd5: true,
	}
	reader := bytes.NewReader(byes)
	objectSize := int64(len(byes))

	if overridePath != "" {
		remotePath = path.Join(overridePath, backupID, key)
	}

	bucket := s.config.Bucket
	if overrideBucket != "" {
		bucket = overrideBucket
	}

	_, err = client.PutObject(ctx, bucket, remotePath, reader, objectSize, opt)
	if err != nil {
		return backup.NewErrInternal(
			errors.Wrapf(err, "put object: %s:%s", bucket, remotePath))
	}

	metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(len(byes)))
	}
	return nil
}

func (s *s3Client) Initialize(ctx context.Context, backupID, overrideBucket, overridePath string) error {
	client, err := s.getClient(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get client")
	}

	key := "access-check"

	if err := s.PutObject(ctx, backupID, key, overrideBucket, overridePath, []byte("")); err != nil {
		return errors.Wrap(err, "failed to access-check s3 backup module")
	}

	objectName := s.makeObjectName(backupID, key)
	opt := minio.RemoveObjectOptions{}
	if err := client.RemoveObject(ctx, s.config.Bucket, objectName, opt); err != nil {
		return errors.Wrap(err, "failed to remove access-check s3 backup module")
	}

	return nil
}

// WriteFile downloads contents of an object to a local file destPath
func (s *s3Client) WriteToFile(ctx context.Context, backupID, key, destPath, overrideBucket, overridePath string) error {
	client, err := s.getClient(ctx)
	if err != nil {
		return errors.Wrap(err, "write to file: cannot get client")
	}
	remotePath := s.makeObjectName(backupID, key)
	if overridePath != "" {
		remotePath = path.Join(overridePath, backupID, key)
	}

	bucket := s.config.Bucket
	if overrideBucket != "" {
		bucket = overrideBucket
	}

	err = client.FGetObject(ctx, bucket, remotePath, destPath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("s3.FGetObject %q %q: %w", destPath, remotePath, err)
	}

	if st, err := os.Stat(destPath); err == nil {
		metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(Name, "class")
		if err == nil {
			metric.Add(float64(st.Size()))
		}
	}
	return nil
}

func (s *s3Client) Write(ctx context.Context, backupID, key, overrideBucket, overridePath string, r io.ReadCloser) (int64, error) {
	defer r.Close()
	client, err := s.getClient(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "write: cannot get client")
	}
	remotePath := s.makeObjectName(backupID, key)
	opt := minio.PutObjectOptions{
		ContentType:      "application/octet-stream",
		DisableMultipart: false,
		PartSize:         MINIO_MIN_PART_SIZE,
		SendContentMd5:   true,
	}

	if overridePath != "" {
		remotePath = path.Join(overridePath, backupID, key)
	}

	bucket := s.config.Bucket
	if overrideBucket != "" {
		bucket = overrideBucket
	}

	info, err := client.PutObject(ctx, bucket, remotePath, r, -1, opt)
	if err != nil {
		return info.Size, fmt.Errorf("write object %q", remotePath)
	}

	if metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.
		GetMetricWithLabelValues(Name, "class"); err == nil {
		metric.Add(float64(float64(info.Size)))
	}
	return info.Size, nil
}

func (s *s3Client) Read(ctx context.Context, backupID, key, overrideBucket, overridePath string, w io.WriteCloser) (int64, error) {
	defer w.Close()
	client, err := s.getClient(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "read: cannot get client")
	}
	remotePath := s.makeObjectName(backupID, key)

	if overridePath != "" {
		remotePath = path.Join(overridePath, backupID, key)
	}

	bucket := s.config.Bucket
	if overrideBucket != "" {
		bucket = overrideBucket
	}

	obj, err := client.GetObject(ctx, bucket, remotePath, minio.GetObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("get object %q: %w", remotePath, err)
	}

	read, err := io.Copy(w, obj)
	if err != nil {
		err = fmt.Errorf("get object %q: %w", remotePath, err)
		var s3Err minio.ErrorResponse
		if errors.As(err, &s3Err) && s3Err.StatusCode == http.StatusNotFound {
			err = backup.NewErrNotFound(err)
		}
		return 0, err
	}

	if metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.
		GetMetricWithLabelValues(Name, "class"); err == nil {
		metric.Add(float64(float64(read)))
	}

	return read, nil
}

func (s *s3Client) SourceDataPath() string {
	return s.dataPath
}
