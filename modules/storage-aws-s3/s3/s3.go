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
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
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
	client *minio.Client
	config Config
	logger logrus.FieldLogger
}

func New(config Config, logger logrus.FieldLogger) (modulecapabilities.SnapshotStorage, error) {
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
	return &s3{client, config, logger}, nil
}

func (s *s3) StoreSnapshot(ctx context.Context, snapshot snapshots.Snapshot) error {
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

		objectName := fmt.Sprintf("%s/%s", snapshotID, srcRelPath)
		filePath := fmt.Sprintf("%s/%s", snapshot.BasePath, srcRelPath)

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
	objectName := fmt.Sprintf("%s/snapshot.json", snapshotID)
	reader := bytes.NewReader(content)
	_, err = s.client.PutObject(ctx, bucketName, objectName, reader, reader.Size(), putOptions)
	if err != nil {
		errors.Wrapf(err, "put file %s", objectName)
	}
	return nil
}

func (s *s3) RestoreSnapshot(ctx context.Context, snapshotId string) error {
	// TODO implement
	s.logger.Errorf("RestoreSnapshot of StorageAWSS3Module not yet implemented")
	return nil
}
