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

package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"github.com/sirupsen/logrus"
)

const (
	AWS_ROLE_ARN                = "AWS_ROLE_ARN"
	AWS_WEB_IDENTITY_TOKEN_FILE = "AWS_WEB_IDENTITY_TOKEN_FILE"
	AWS_REGION                  = "AWS_REGION"
	AWS_DEFAULT_REGION          = "AWS_DEFAULT_REGION"
)

type s3 struct {
	client   *minio.Client
	config   Config
	logger   logrus.FieldLogger
	dataPath string
}

func New(config Config, logger logrus.FieldLogger, dataPath string) (*s3, error) {
	region := os.Getenv(AWS_REGION)
	if len(region) == 0 {
		region = os.Getenv(AWS_DEFAULT_REGION)
	}
	creds := credentials.NewEnvAWS()
	if len(os.Getenv(AWS_WEB_IDENTITY_TOKEN_FILE)) > 0 && len(os.Getenv(AWS_ROLE_ARN)) > 0 {
		creds = credentials.NewIAM("")
	}
	client, err := minio.New(config.Endpoint(), &minio.Options{
		Creds:  creds,
		Region: region,
		Secure: config.UseSSL(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "create client")
	}
	return &s3{client, config, logger, dataPath}, nil
}

func (s *s3) makeObjectName(parts ...string) string {
	base := path.Join(parts...)
	return path.Join(s.config.BackupPath(), base)
}

func (s *s3) HomeDir(backupID string) string {
	return "s3://" + path.Join(s.config.BucketName(),
		s.makeObjectName(backupID))
}

func (s *s3) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	objectName := s.makeObjectName(backupID, key)

	if err := ctx.Err(); err != nil {
		return nil, backup.NewErrContextExpired(errors.Wrapf(err, "get object '%s'", objectName))
	}

	obj, err := s.client.GetObject(ctx, s.config.BucketName(), objectName, minio.GetObjectOptions{})
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

	monitoring.GetMetrics().BackupRestoreDataTransferred.WithLabelValues("backup-s3").Add(float64(len(contents)))

	return contents, nil
}

func (s *s3) PutFile(ctx context.Context, backupID, key string, srcPath string) error {
	objectName := s.makeObjectName(backupID, key)
	srcPath = path.Join(s.dataPath, srcPath)
	opt := minio.PutObjectOptions{ContentType: "application/octet-stream"}

	_, err := s.client.FPutObject(ctx, s.config.BucketName(), objectName, srcPath, opt)
	if err != nil {
		return backup.NewErrInternal(
			errors.Wrapf(err, "put file '%s'", objectName))
	}

	return nil
}

func (s *s3) PutObject(ctx context.Context, backupID, key string, byes []byte) error {
	objectName := s.makeObjectName(backupID, key)
	opt := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	reader := bytes.NewReader(byes)
	objectSize := int64(len(byes))

	_, err := s.client.PutObject(ctx, s.config.BucketName(), objectName, reader, objectSize, opt)
	if err != nil {
		return backup.NewErrInternal(
			errors.Wrapf(err, "put object '%s'", objectName))
	}

	monitoring.GetMetrics().BackupStoreDataTransferred.WithLabelValues("backup-s3").Add(float64(len(byes)))
	return nil
}

func (s *s3) Initialize(ctx context.Context, backupID string) error {
	key := "access-check"

	if err := s.PutObject(ctx, backupID, key, []byte("")); err != nil {
		return errors.Wrap(err, "failed to access-check s3 backup module")
	}

	objectName := s.makeObjectName(backupID, key)
	opt := minio.RemoveObjectOptions{}
	if err := s.client.RemoveObject(ctx, s.config.BucketName(), objectName, opt); err != nil {
		return errors.Wrap(err, "failed to remove access-check s3 backup module")
	}

	return nil
}

func (s *s3) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	// TODO use s.client.FGetObject() because it is more efficient than GetObject
	obj, err := s.GetObject(ctx, backupID, key)
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

func (s *s3) SourceDataPath() string {
	return s.dataPath
}
