//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package docker

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/testcontainers/testcontainers-go"
	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	modstgfilesystem "github.com/weaviate/weaviate/modules/backup-filesystem"
	modstggcs "github.com/weaviate/weaviate/modules/backup-gcs"
	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"
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
	// envTestMulti2VecCLIPImage adds ability to pass a custom CLIP image to module tests
	envTestMulti2VecCLIPImage = "TEST_MULTI2VEC_CLIP_IMAGE"
)

const (
	Ref2VecCentroid = "ref2vec-centroid"
)

type Compose struct {
	enableModules             []string
	defaultVectorizerModule   string
	withMinIO                 bool
	withGCS                   bool
	withAzurite               bool
	withBackendFilesystem     bool
	withBackendS3             bool
	withBackendS3Bucket       string
	withBackendGCS            bool
	withBackendGCSBucket      string
	withBackendAzure          bool
	withBackendAzureContainer string
	withTransformers          bool
	withContextionary         bool
	withQnATransformers       bool
	withWeaviate              bool
	withWeaviateAuth          bool
	withWeaviateCluster       bool
	withSUMTransformers       bool
	withCentroid              bool
	withCLIP                  bool
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

func (d *Compose) WithAzurite() *Compose {
	d.withAzurite = true
	d.enableModules = append(d.enableModules, modstgazure.Name)
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

func (d *Compose) WithBackendFilesystem() *Compose {
	d.withBackendFilesystem = true
	d.enableModules = append(d.enableModules, modstgfilesystem.Name)
	return d
}

func (d *Compose) WithBackendS3(bucket string) *Compose {
	d.withBackendS3 = true
	d.withBackendS3Bucket = bucket
	d.withMinIO = true
	d.enableModules = append(d.enableModules, modstgs3.Name)
	return d
}

func (d *Compose) WithBackendGCS(bucket string) *Compose {
	d.withBackendGCS = true
	d.withBackendGCSBucket = bucket
	d.withGCS = true
	d.enableModules = append(d.enableModules, modstggcs.Name)
	return d
}

func (d *Compose) WithBackendAzure(container string) *Compose {
	d.withBackendAzure = true
	d.withBackendAzureContainer = container
	d.withAzurite = true
	d.enableModules = append(d.enableModules, modstgazure.Name)
	return d
}

func (d *Compose) WithSUMTransformers() *Compose {
	d.withSUMTransformers = true
	d.enableModules = append(d.enableModules, SUMTransformers)
	return d
}

func (d *Compose) WithMulti2VecCLIP() *Compose {
	d.withCLIP = true
	d.enableModules = append(d.enableModules, Multi2VecCLIP)
	return d
}

func (d *Compose) WithRef2VecCentroid() *Compose {
	d.withCentroid = true
	d.enableModules = append(d.enableModules, Ref2VecCentroid)
	return d
}

func (d *Compose) WithWeaviate() *Compose {
	d.withWeaviate = true
	return d
}

func (d *Compose) WithWeaviateCluster() *Compose {
	d.withWeaviate = true
	d.withWeaviateCluster = true
	return d
}

func (d *Compose) WithWeaviateAuth() *Compose {
	d.withWeaviate = true
	d.withWeaviateAuth = true
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
		if d.withBackendS3 {
			for k, v := range container.envSettings {
				envSettings[k] = v
			}
			envSettings["BACKUP_S3_BUCKET"] = d.withBackendS3Bucket
		}
	}
	if d.withGCS {
		container, err := startGCS(ctx, networkName)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", GCS)
		}
		containers = append(containers, container)
		if d.withBackendGCS {
			for k, v := range container.envSettings {
				envSettings[k] = v
			}
			envSettings["BACKUP_GCS_BUCKET"] = d.withBackendGCSBucket
		}
	}
	if d.withAzurite {
		container, err := startAzurite(ctx, networkName)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", Azurite)
		}
		containers = append(containers, container)
		if d.withBackendAzure {
			for k, v := range container.envSettings {
				envSettings[k] = v
			}
			envSettings["BACKUP_AZURE_CONTAINER"] = d.withBackendAzureContainer
		}
	}
	if d.withBackendFilesystem {
		envSettings["BACKUP_FILESYSTEM_PATH"] = "/tmp/backups"
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
	if d.withCLIP {
		image := os.Getenv(envTestMulti2VecCLIPImage)
		container, err := startM2VClip(ctx, networkName, image)
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
		hostname := Weaviate
		if d.withWeaviateCluster {
			envSettings["CLUSTER_HOSTNAME"] = "node1"
			envSettings["CLUSTER_GOSSIP_BIND_PORT"] = "7100"
			envSettings["CLUSTER_DATA_BIND_PORT"] = "7101"
		}
		if d.withWeaviateAuth {
			envSettings["AUTHENTICATION_OIDC_ENABLED"] = "true"
			envSettings["AUTHENTICATION_OIDC_CLIENT_ID"] = "wcs"
			envSettings["AUTHENTICATION_OIDC_ISSUER"] = "https://auth.wcs.api.semi.technology/auth/realms/SeMI"
			envSettings["AUTHENTICATION_OIDC_USERNAME_CLAIM"] = "email"
			envSettings["AUTHENTICATION_OIDC_GROUPS_CLAIM"] = "groups"
			envSettings["AUTHORIZATION_ADMINLIST_ENABLED"] = "true"
			envSettings["AUTHORIZATION_ADMINLIST_USERS"] = "ms_2d0e007e7136de11d5f29fce7a53dae219a51458@existiert.net"
		}
		container, err := startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule,
			envSettings, networkName, image, hostname)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", hostname)
		}
		containers = append(containers, container)
	}
	if d.withWeaviateCluster {
		image := os.Getenv(envTestWeaviateImage)
		hostname := WeaviateNode2
		envSettings["CLUSTER_HOSTNAME"] = "node2"
		envSettings["CLUSTER_GOSSIP_BIND_PORT"] = "7102"
		envSettings["CLUSTER_DATA_BIND_PORT"] = "7103"
		envSettings["CLUSTER_JOIN"] = fmt.Sprintf("%s:7100", Weaviate)
		container, err := startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule,
			envSettings, networkName, image, hostname)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", hostname)
		}
		containers = append(containers, container)
	}

	return &DockerCompose{network, containers}, nil
}
