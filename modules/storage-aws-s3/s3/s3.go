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
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
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

func (s *s3) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	// create bucket
	bucketName := s.config.BucketName()
	bucketExists, err := s.client.BucketExists(ctx, bucketName)
	if err != nil {
		return errors.Wrap(err, "bucket exists")
	}
	if !bucketExists {
		err = s.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return errors.Wrapf(err, "create bucket %s", bucketName)
		}
	}
	// save files
	snapshotID := snapshot.ID
	putOptions := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "store snapshot aborted")
		}

		objectName := s.makeObjectName(snapshotID, srcRelPath)
		filePath := s.makeFilePath(s.dataPath, srcRelPath)

		_, err := s.client.FPutObject(ctx, bucketName, objectName, filePath, putOptions)
		if err != nil {
			errors.Wrapf(err, "put file %s", objectName)
		}
	}
	// save meta
	content, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Wrapf(err, "save meta")
	}
	objectName := s.makeObjectName(snapshotID, "snapshot.json")
	reader := bytes.NewReader(content)
	_, err = s.client.PutObject(ctx, bucketName, objectName, reader, reader.Size(), putOptions)
	if err != nil {
		errors.Wrapf(err, "put file %s", objectName)
	}
	return nil
}

func (s *s3) makeFilePath(dataPath, relPath string) string {
	return fmt.Sprintf("%s/%s", dataPath, relPath)
}

func (s *s3) makeObjectName(snapshotId, srcRelPath string) string {
	return fmt.Sprintf("%s/%s", snapshotId, srcRelPath)
}

// TODO handle className
func (s *s3) RestoreSnapshot(ctx context.Context, className, snapshotId string) error {
	bucketName := s.config.BucketName()
	bucketExists, err := s.client.BucketExists(ctx, bucketName)
	if !bucketExists {
		return errors.Wrap(err, "backup bucket does not exist")
	}
	if err != nil {
		return errors.Wrap(err, "can't connect to bucket")
	}

	objectName := fmt.Sprintf("%s/%s", snapshotId, "snapshot.json")

	// Load the metadata from the backup into a snapshot struct
	var snapshot snapshots.Snapshot
	val, err := s.client.GetObject(ctx, s.config.BucketName(), objectName, minio.GetObjectOptions{})
	if err != nil {
		return errors.Wrapf(err, "get file %s", objectName)
	}
	data, err := io.ReadAll(val)
	if err != nil {
		return errors.Wrapf(err, "read file %s", objectName)
	}
	err = json.Unmarshal(data, &snapshot)
	if err != nil {
		return errors.Wrapf(err, "unmarshal meta")
	}

	// Restore the files
	for _, srcRelPath := range snapshot.Files {
		if err := ctx.Err(); err != nil {
			return errors.Wrapf(err, "restore snapshot aborted")
		}

		// Get the correct paths for the backup file and the active file
		objectName := s.makeObjectName(snapshotId, srcRelPath)
		filePath := s.makeFilePath(s.dataPath, srcRelPath)

		// Download the backup file from the bucket
		err := s.client.FGetObject(ctx, s.config.BucketName(), objectName, filePath, minio.GetObjectOptions{})
		if err != nil {
			return errors.Wrapf(err, "Unable to restore file %s, system might be in a corrupted state", filePath)
		}
	}
	return nil
}

func (s *s3) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	// TODO implement
	return nil
}

func (s *s3) GetMetaStatus(ctx context.Context, className, snapshotID string) (string, error) {
	// TODO implement
	return "", nil
}

func (s *s3) DestinationPath(className, snapshotID string) string {
	// TODO implement
	return ""
}
