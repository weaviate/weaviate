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
	"fmt"
	"io"
	"net/http"
	"os"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type s3Client struct {
	client   *minio.Client
	config   *clientConfig
	logger   logrus.FieldLogger
	dataPath string
	bucket   string
	path     string
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
		if _, err := creds.Get(); err != nil {
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
	return &s3Client{client, config, logger, dataPath, bucket, path}, nil
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

func (s *s3Client) GetObject(ctx context.Context, backupID, key, bucketName, bucketPath string) ([]byte, error) {
	remotePath := s.makeObjectName(backupID, key)

	if bucketPath != "" {
		remotePath = path.Join(bucketPath, backupID, key)
	}

	bucket := s.config.Bucket
	if bucketName != "" {
		bucket = bucketName
	}

	if err := ctx.Err(); err != nil {
		return nil, backup.NewErrContextExpired(errors.Wrapf(err, "context expired in get object '%s'", remotePath))
	}

	fmt.Printf("!!!Get object '%s' from bucket '%s'\n", remotePath, bucket)
	obj, err := s.client.GetObject(ctx, bucket, remotePath, minio.GetObjectOptions{})
	if err != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object '%s'", remotePath))
	}

	contents, err := io.ReadAll(obj)
	if err != nil {
		if s3Err, ok := err.(minio.ErrorResponse); ok && s3Err.StatusCode == http.StatusNotFound {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object contents from %s:%s not found '%s'", bucket, remotePath, remotePath))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object contents from %s:%s '%s'", bucket, remotePath, remotePath))
	}

	metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(len(contents)))
	}

	return contents, nil
}

func (s *s3Client) PutObject(ctx context.Context, backupID, key, bucketName, bucketPath string, byes []byte) error {
	remotePath := s.makeObjectName(backupID, key)
	opt := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	reader := bytes.NewReader(byes)
	objectSize := int64(len(byes))

	if bucketPath != "" {
		remotePath = path.Join(bucketPath, backupID, key)
	}

	bucket := s.config.Bucket
	if bucketName != "" {
		bucket = bucketName
	}

	fmt.Printf("!!!Put object '%s' to bucket '%s'\n", remotePath, bucket)
	_, err := s.client.PutObject(ctx, bucket, remotePath, reader, objectSize, opt)
	if err != nil {
		return backup.NewErrInternal(
			errors.Wrapf(err, "put object '%s:%s'", bucket, remotePath))
	}

	fmt.Printf("Put object '%s' to bucket '%s'\n", remotePath, bucket)
	metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(len(byes)))
	}
	return nil
}

func (s *s3Client) Initialize(ctx context.Context, backupID, bucketName, bucketPath string) error {
	key := "access-check"

	if err := s.PutObject(ctx, backupID, key, bucketName, bucketPath, []byte("")); err != nil {
		return errors.Wrap(err, "failed to access-check s3 backup module")
	}

	objectName := s.makeObjectName(backupID, key)
	opt := minio.RemoveObjectOptions{}
	if err := s.client.RemoveObject(ctx, s.config.Bucket, objectName, opt); err != nil {
		return errors.Wrap(err, "failed to remove access-check s3 backup module")
	}

	return nil
}

// WriteFile downloads contents of an object to a local file destPath
func (s *s3Client) WriteToFile(ctx context.Context, backupID, key, destPath, bucketName, bucketPath string) error {
	remotePath := s.makeObjectName(backupID, key)
	if bucketPath != "" {
		remotePath = path.Join(bucketPath, backupID, key)
	}

	bucket := s.config.Bucket
	if bucketName != "" {
		bucket = bucketName
	}

	err := s.client.FGetObject(ctx, bucket, remotePath, destPath, minio.GetObjectOptions{})
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

func (s *s3Client) Write(ctx context.Context, backupID, key, bucketName, bucketPath string, r io.ReadCloser) (int64, error) {
	defer r.Close()
	remotePath := s.makeObjectName(backupID, key)
	opt := minio.PutObjectOptions{
		ContentType:      "application/octet-stream",
		DisableMultipart: false,
	}

	if bucketPath != "" {
		remotePath = path.Join(bucketPath, backupID, key)
	}

	bucket := s.config.Bucket
	if bucketName != "" {
		bucket = bucketName
	}

	info, err := s.client.PutObject(ctx, bucket, remotePath, r, -1, opt)
	if err != nil {
		return info.Size, fmt.Errorf("write object %q", remotePath)
	}

	if metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.
		GetMetricWithLabelValues(Name, "class"); err == nil {
		metric.Add(float64(float64(info.Size)))
	}
	return info.Size, nil
}

func (s *s3Client) Read(ctx context.Context, backupID, key, bucketName, bucketPath string, w io.WriteCloser) (int64, error) {
	defer w.Close()
	remotePath := s.makeObjectName(backupID, key)

	if bucketPath != "" {
		remotePath = path.Join(bucketPath, backupID, key)
	}

	bucket := s.config.Bucket
	if bucketName != "" {
		bucket = bucketName
	}

	obj, err := s.client.GetObject(ctx, bucket, remotePath, minio.GetObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("get object %q: %w", remotePath, err)
	}

	read, err := io.Copy(w, obj)
	if err != nil {
		err = fmt.Errorf("get object %q: %w", remotePath, err)
		if s3Err, ok := err.(minio.ErrorResponse); ok && s3Err.StatusCode == http.StatusNotFound {
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
