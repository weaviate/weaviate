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
}

func newClient(config *clientConfig, logger logrus.FieldLogger, dataPath string) (*s3Client, error) {
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
	return &s3Client{client, config, logger, dataPath}, nil
}

func (s *s3Client) makeObjectName(parts ...string) string {
	base := path.Join(parts...)
	return path.Join(s.config.BackupPath, base)
}

func (s *s3Client) HomeDir(backupID string) string {
	return "s3://" + path.Join(s.config.Bucket,
		s.makeObjectName(backupID))
}

func (s *s3Client) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	objectName := s.makeObjectName(backupID, key)

	if err := ctx.Err(); err != nil {
		return nil, backup.NewErrContextExpired(errors.Wrapf(err, "get object '%s'", objectName))
	}

	obj, err := s.client.GetObject(ctx, s.config.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object '%s'", objectName))
	}

	contents, err := io.ReadAll(obj)
	if err != nil {
		if s3Err, ok := err.(minio.ErrorResponse); ok && s3Err.StatusCode == http.StatusNotFound {
			return nil, backup.NewErrNotFound(errors.Wrapf(err, "get object '%s'", objectName))
		}
		return nil, backup.NewErrInternal(errors.Wrapf(err, "get object '%s'", objectName))
	}

	metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(len(contents)))
	}

	return contents, nil
}

func (s *s3Client) PutFile(ctx context.Context, backupID, key string, srcPath string) error {
	objectName := s.makeObjectName(backupID, key)
	srcPath = path.Join(s.dataPath, srcPath)
	opt := minio.PutObjectOptions{ContentType: "application/octet-stream"}

	_, err := s.client.FPutObject(ctx, s.config.Bucket, objectName, srcPath, opt)
	if err != nil {
		return backup.NewErrInternal(
			errors.Wrapf(err, "put file '%s'", objectName))
	}

	// Get filesize
	file, err := os.Stat(srcPath)
	if err != nil {
		return nil
	}
	size := file.Size()

	metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(size))
	}
	return nil
}

func (s *s3Client) PutObject(ctx context.Context, backupID, key string, byes []byte) error {
	objectName := s.makeObjectName(backupID, key)
	opt := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	reader := bytes.NewReader(byes)
	objectSize := int64(len(byes))

	_, err := s.client.PutObject(ctx, s.config.Bucket, objectName, reader, objectSize, opt)
	if err != nil {
		return backup.NewErrInternal(
			errors.Wrapf(err, "put object '%s'", objectName))
	}

	metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.GetMetricWithLabelValues(Name, "class")
	if err == nil {
		metric.Add(float64(len(byes)))
	}
	return nil
}

func (s *s3Client) Initialize(ctx context.Context, backupID string) error {
	key := "access-check"

	if err := s.PutObject(ctx, backupID, key, []byte("")); err != nil {
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
func (s *s3Client) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	object := s.makeObjectName(backupID, key)
	err := s.client.FGetObject(ctx, s.config.Bucket, object, destPath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("s3.FGetObject %q %q: %w", destPath, object, err)
	}

	if st, err := os.Stat(destPath); err == nil {
		metric, err := monitoring.GetMetrics().BackupRestoreDataTransferred.GetMetricWithLabelValues(Name, "class")
		if err == nil {
			metric.Add(float64(st.Size()))
		}
	}
	return nil
}

func (s *s3Client) Write(ctx context.Context, backupID, key string, r io.ReadCloser) (int64, error) {
	defer r.Close()
	path := s.makeObjectName(backupID, key)
	opt := minio.PutObjectOptions{
		ContentType:      "application/octet-stream",
		DisableMultipart: false,
	}

	info, err := s.client.PutObject(ctx, s.config.Bucket, path, r, -1, opt)
	if err != nil {
		return info.Size, fmt.Errorf("write object %q", path)
	}

	if metric, err := monitoring.GetMetrics().BackupStoreDataTransferred.
		GetMetricWithLabelValues(Name, "class"); err == nil {
		metric.Add(float64(float64(info.Size)))
	}
	return info.Size, nil
}

func (s *s3Client) Read(ctx context.Context, backupID, key string, w io.WriteCloser) (int64, error) {
	defer w.Close()
	path := s.makeObjectName(backupID, key)
	obj, err := s.client.GetObject(ctx, s.config.Bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("get object %q: %w", path, err)
	}

	read, err := io.Copy(w, obj)
	if err != nil {
		err = fmt.Errorf("get object %q: %w", path, err)
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
