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

package docker

import (
	"context"
	"os"

	"github.com/pkg/errors"
	modstgs3 "github.com/semi-technologies/weaviate/modules/storage-aws-s3"
	modstggcs "github.com/semi-technologies/weaviate/modules/storage-gcs"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// envTestWeaviateImage can be passed to tests to spin up docker compose with given image
	envTestWeaviateImage = "TEST_WEAVIATE_IMAGE"
	// envTestText2vecTransformersImage adds ability to pass a custom image to module tests
	envTestText2vecTransformersImage = "TEST_TEXT2VEC_TRANSFORMERS_IMAGE"
	// envTestText2vecContextionaryImage adds ability to pass a custom image to module tests
	envTestText2vecContextionaryImage = "TEST_TEXT2VEC_CONTEXTIONARY_IMAGE"
	// envTestQnATransformersImage adds ability to pass a custom image to module tests
	envTestQnATransformersImage = "TEST_QNA_TRANSFORMERS_IMAGE"
	// envTestSUMTransformersImage adds ability to pass a custom image to module tests
	envTestSUMTransformersImage = "TEST_SUM_TRANSFORMERS_IMAGE"
)

const (
	StorageFileSystem = "storage-filesystem"
	StorageAWSS3      = "storage-aws-s3"
	StorageGCS        = "storage-gcs"
)

type Compose struct {
	enableModules           []string
	defaultVectorizerModule string
	withMinIO               bool
	withGCS                 bool
	withStorageFilesystem   bool
	withStorageAWSS3        bool
	withStorageAWSS3Bucket  string
	withStorageGCS          bool
	withStorageGCSBucket    string
	withTransformers        bool
	withContextionary       bool
	withQnATransformers     bool
	withWeaviate            bool
	withSUMTransformers     bool
}

func New() *Compose {
	return &Compose{enableModules: []string{}}
}

func (d *Compose) WithMinIO() *Compose {
	d.withMinIO = true
	d.enableModules = append(d.enableModules, modstgs3.Name)
	return d
}

func (d *Compose) WithGCS() *Compose {
	d.withGCS = true
	d.enableModules = append(d.enableModules, modstggcs.Name)
	return d
}

func (d *Compose) WithText2VecTransformers() *Compose {
	d.withTransformers = true
	d.enableModules = append(d.enableModules, Text2VecTransformers)
	d.defaultVectorizerModule = Text2VecTransformers
	return d
}

func (d *Compose) WithText2VecContextionary() *Compose {
	d.withContextionary = true
	d.enableModules = append(d.enableModules, Text2VecContextionary)
	d.defaultVectorizerModule = Text2VecContextionary
	return d
}

func (d *Compose) WithQnATransformers() *Compose {
	d.withQnATransformers = true
	d.enableModules = append(d.enableModules, QnATransformers)
	return d
}

func (d *Compose) WithStorageFilesystem() *Compose {
	d.withStorageFilesystem = true
	d.enableModules = append(d.enableModules, StorageFileSystem)
	return d
}

func (d *Compose) WithStorageAWSS3(bucket string) *Compose {
	d.withStorageAWSS3 = true
	d.withStorageAWSS3Bucket = bucket
	d.withMinIO = true
	d.enableModules = append(d.enableModules, StorageAWSS3)
	return d
}

func (d *Compose) WithStorageGCS(bucket string) *Compose {
	d.withStorageGCS = true
	d.withStorageGCSBucket = bucket
	d.withGCS = true
	d.enableModules = append(d.enableModules, StorageGCS)
	return d
}

func (d *Compose) WithSUMTransformers() *Compose {
	d.withSUMTransformers = true
	d.enableModules = append(d.enableModules, SUMTransformers)
	return d
}

func (d *Compose) WithWeaviate() *Compose {
	d.withWeaviate = true
	return d
}

func (d *Compose) Start(ctx context.Context) (*DockerCompose, error) {
	networkName := "weaviate-module-acceptance-tests"
	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:     networkName,
			Internal: false,
		},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "network: %s", networkName)
	}
	envSettings := make(map[string]string)
	containers := []*DockerContainer{}
	if d.withMinIO {
		container, err := startMinIO(ctx, networkName)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", MinIO)
		}
		containers = append(containers, container)
		if d.withStorageAWSS3 {
			for k, v := range container.envSettings {
				envSettings[k] = v
			}
			envSettings["STORAGE_S3_BUCKET"] = d.withStorageAWSS3Bucket
		}
	}
	if d.withGCS {
		container, err := startGCS(ctx, networkName)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", GCS)
		}
		containers = append(containers, container)
		if d.withStorageGCS {
			for k, v := range container.envSettings {
				envSettings[k] = v
			}
			envSettings["STORAGE_GCS_BUCKET"] = d.withStorageGCSBucket
		}
	}
	if d.withStorageFilesystem {
		envSettings["STORAGE_FS_PATH"] = "/tmp/snapshots"
	}
	if d.withTransformers {
		image := os.Getenv(envTestText2vecTransformersImage)
		container, err := startT2VTransformers(ctx, networkName, image)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", Text2VecTransformers)
		}
		for k, v := range container.envSettings {
			envSettings[k] = v
		}
		containers = append(containers, container)
	}
	if d.withContextionary {
		image := os.Getenv(envTestText2vecContextionaryImage)
		container, err := startT2VContextionary(ctx, networkName, image)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", Text2VecContextionary)
		}
		for k, v := range container.envSettings {
			envSettings[k] = v
		}
		containers = append(containers, container)
	}
	if d.withQnATransformers {
		image := os.Getenv(envTestQnATransformersImage)
		container, err := startQnATransformers(ctx, networkName, image)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", QnATransformers)
		}
		for k, v := range container.envSettings {
			envSettings[k] = v
		}
		containers = append(containers, container)
	}
	if d.withSUMTransformers {
		image := os.Getenv(envTestSUMTransformersImage)
		container, err := startSUMTransformers(ctx, networkName, image)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", SUMTransformers)
		}
		for k, v := range container.envSettings {
			envSettings[k] = v
		}
		containers = append(containers, container)
	}
	if d.withWeaviate {
		image := os.Getenv(envTestWeaviateImage)
		container, err := startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule,
			envSettings, networkName, image)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", Weaviate)
		}
		containers = append(containers, container)
	}

	return &DockerCompose{network, containers}, nil
}
