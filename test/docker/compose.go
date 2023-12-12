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
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/testcontainers/testcontainers-go"
	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	modstgfilesystem "github.com/weaviate/weaviate/modules/backup-filesystem"
	modstggcs "github.com/weaviate/weaviate/modules/backup-gcs"
	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"
	modgenerativeanyscale "github.com/weaviate/weaviate/modules/generative-anyscale"
	modgenerativeaws "github.com/weaviate/weaviate/modules/generative-aws"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modgenerativepalm "github.com/weaviate/weaviate/modules/generative-palm"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	modaws "github.com/weaviate/weaviate/modules/text2vec-aws"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modpalm "github.com/weaviate/weaviate/modules/text2vec-palm"
	"golang.org/x/sync/errgroup"
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
	// envTestImg2VecNeuralImage adds ability to pass a custom Im2Vec Neural image to module tests
	envTestImg2VecNeuralImage = "TEST_IMG2VEC_NEURAL_IMAGE"
	// envTestRerankerTransformersImage adds ability to pass a custom image to module tests
	envTestRerankerTransformersImage = "TEST_RERANKER_TRANSFORMERS_IMAGE"
)

const (
	Ref2VecCentroid = "ref2vec-centroid"
)

type Compose struct {
	enableModules              []string
	defaultVectorizerModule    string
	withMinIO                  bool
	withGCS                    bool
	withAzurite                bool
	withBackendFilesystem      bool
	withBackendS3              bool
	withBackendS3Bucket        string
	withBackendGCS             bool
	withBackendGCSBucket       string
	withBackendAzure           bool
	withBackendAzureContainer  string
	withTransformers           bool
	withContextionary          bool
	withQnATransformers        bool
	withWeaviate               bool
	withWeaviateExposeGRPCPort bool
	withSecondWeaviate         bool
	size                       int

	withWeaviateAuth              bool
	withWeaviateBasicAuth         bool
	withWeaviateBasicAuthUsername string
	withWeaviateBasicAuthPassword string
	withWeaviateCluster           bool
	withSUMTransformers           bool
	withCentroid                  bool
	withCLIP                      bool
	withImg2Vec                   bool
	withRerankerTransformers      bool
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

func (d *Compose) WithImg2VecNeural() *Compose {
	d.withImg2Vec = true
	d.enableModules = append(d.enableModules, Img2VecNeural)
	return d
}

func (d *Compose) WithRef2VecCentroid() *Compose {
	d.withCentroid = true
	d.enableModules = append(d.enableModules, Ref2VecCentroid)
	return d
}

func (d *Compose) WithText2VecOpenAI() *Compose {
	d.enableModules = append(d.enableModules, modopenai.Name)
	return d
}

func (d *Compose) WithText2VecCohere() *Compose {
	d.enableModules = append(d.enableModules, modcohere.Name)
	return d
}

func (d *Compose) WithText2VecPaLM() *Compose {
	d.enableModules = append(d.enableModules, modpalm.Name)
	return d
}

func (d *Compose) WithText2VecAWS() *Compose {
	d.enableModules = append(d.enableModules, modaws.Name)
	return d
}

func (d *Compose) WithText2VecHuggingFace() *Compose {
	d.enableModules = append(d.enableModules, modhuggingface.Name)
	return d
}

func (d *Compose) WithGenerativeOpenAI() *Compose {
	d.enableModules = append(d.enableModules, modgenerativeopenai.Name)
	return d
}

func (d *Compose) WithGenerativeAWS() *Compose {
	d.enableModules = append(d.enableModules, modgenerativeaws.Name)
	return d
}

func (d *Compose) WithGenerativeCohere() *Compose {
	d.enableModules = append(d.enableModules, modgenerativecohere.Name)
	return d
}

func (d *Compose) WithGenerativePaLM() *Compose {
	d.enableModules = append(d.enableModules, modgenerativepalm.Name)
	return d
}

func (d *Compose) WithGenerativeAnyscale() *Compose {
	d.enableModules = append(d.enableModules, modgenerativeanyscale.Name)
	return d
}

func (d *Compose) WithQnAOpenAI() *Compose {
	d.enableModules = append(d.enableModules, modqnaopenai.Name)
	return d
}

func (d *Compose) WithRerankerCohere() *Compose {
	d.enableModules = append(d.enableModules, modrerankercohere.Name)
	return d
}

func (d *Compose) WithRerankerTransformers() *Compose {
	d.withRerankerTransformers = true
	d.enableModules = append(d.enableModules, RerankerTransformers)
	return d
}

func (d *Compose) WithWeaviate() *Compose {
	d.withWeaviate = true
	return d
}

func (d *Compose) WithWeaviateWithGRPC() *Compose {
	d.withWeaviate = true
	d.withWeaviateExposeGRPCPort = true
	return d
}

func (d *Compose) WithSecondWeaviate() *Compose {
	d.withWeaviate = true
	d.withWeaviateCluster = false
	d.withSecondWeaviate = true
	return d
}

func (d *Compose) WithWeaviateCluster() *Compose {
	return d.With2NodeCluster()
}

func (d *Compose) WithWeaviateClusterWithGRPC() *Compose {
	d.withWeaviate = true
	d.withWeaviateCluster = true
	d.withWeaviateExposeGRPCPort = true
	return d
}

func (d *Compose) WithBasicAuth(username, password string) *Compose {
	d.withWeaviateBasicAuth = true
	d.withWeaviateBasicAuthUsername = username
	d.withWeaviateBasicAuthPassword = password
	return d
}

func (d *Compose) WithWeaviateAuth() *Compose {
	return d.With1NodeCluster()
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
	envSettings["network"] = networkName
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
			return nil, errors.Wrapf(err, "start %s", Multi2VecCLIP)
		}
		for k, v := range container.envSettings {
			envSettings[k] = v
		}
		containers = append(containers, container)
	}
	if d.withImg2Vec {
		image := os.Getenv(envTestImg2VecNeuralImage)
		container, err := startI2VNeural(ctx, networkName, image)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", Img2VecNeural)
		}
		for k, v := range container.envSettings {
			envSettings[k] = v
		}
		containers = append(containers, container)
	}
	if d.withRerankerTransformers {
		image := os.Getenv(envTestRerankerTransformersImage)
		container, err := startRerankerTransformers(ctx, networkName, image)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", RerankerTransformers)
		}
		for k, v := range container.envSettings {
			envSettings[k] = v
		}
		containers = append(containers, container)
	}
	if d.withWeaviateCluster {
		cs, err := d.startCluster(ctx, d.size, envSettings)
		for _, c := range cs {
			if c != nil {
				containers = append(containers, c)
			}
		}
		return &DockerCompose{network, containers}, err
	}

	if d.withWeaviate {
		image := os.Getenv(envTestWeaviateImage)
		hostname := Weaviate1
		if d.withWeaviateBasicAuth {
			envSettings["CLUSTER_BASIC_AUTH_USERNAME"] = d.withWeaviateBasicAuthUsername
			envSettings["CLUSTER_BASIC_AUTH_PASSWORD"] = d.withWeaviateBasicAuthPassword
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
			envSettings, networkName, image, hostname, d.withWeaviateExposeGRPCPort)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", hostname)
		}
		containers = append(containers, container)
	}

	if d.withSecondWeaviate {
		image := os.Getenv(envTestWeaviateImage)
		hostname := SecondWeaviate
		secondWeaviateSettings := envSettings
		// Ensure second weaviate doesn't get cluster settings from the first cluster if any.
		delete(secondWeaviateSettings, "CLUSTER_HOSTNAME")
		delete(secondWeaviateSettings, "CLUSTER_GOSSIP_BIND_PORT")
		delete(secondWeaviateSettings, "CLUSTER_DATA_BIND_PORT")
		delete(secondWeaviateSettings, "CLUSTER_JOIN")
		delete(secondWeaviateSettings, "RAFT_PORT")
		delete(secondWeaviateSettings, "RAFT_INTERNAL_PORT")
		delete(secondWeaviateSettings, "RAFT_JOIN")
		container, err := startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule, envSettings, networkName, image, hostname, d.withWeaviateExposeGRPCPort)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", hostname)
		}
		containers = append(containers, container)
	}

	return &DockerCompose{network, containers}, nil
}

func (d *Compose) With1NodeCluster() *Compose {
	d.withWeaviateCluster = true
	d.size = 1
	return d
}

func (d *Compose) With2NodeCluster() *Compose {
	d.withWeaviateCluster = true
	d.size = 2
	return d
}

func (d *Compose) With3NodeCluster() *Compose {
	d.withWeaviateCluster = true
	d.size = 3
	return d
}

func (d *Compose) startCluster(ctx context.Context, size int, settings map[string]string) ([]*DockerContainer, error) {
	if size == 0 || size > 3 {
		return nil, nil
	}

	raft_join := "node1,node2,node3"
	if size == 1 {
		raft_join = "node1"
	} else if size == 2 {
		raft_join = "node1,node2"
	}

	cs := make([]*DockerContainer, size)
	image := os.Getenv(envTestWeaviateImage)
	networkName := settings["network"]
	if d.withWeaviateBasicAuth {
		settings["CLUSTER_BASIC_AUTH_USERNAME"] = d.withWeaviateBasicAuthUsername
		settings["CLUSTER_BASIC_AUTH_PASSWORD"] = d.withWeaviateBasicAuthPassword
	}
	if d.withWeaviateAuth {
		settings["AUTHENTICATION_OIDC_ENABLED"] = "true"
		settings["AUTHENTICATION_OIDC_CLIENT_ID"] = "wcs"
		settings["AUTHENTICATION_OIDC_ISSUER"] = "https://auth.wcs.api.semi.technology/auth/realms/SeMI"
		settings["AUTHENTICATION_OIDC_USERNAME_CLAIM"] = "email"
		settings["AUTHENTICATION_OIDC_GROUPS_CLAIM"] = "groups"
		settings["AUTHORIZATION_ADMINLIST_ENABLED"] = "true"
		settings["AUTHORIZATION_ADMINLIST_USERS"] = "ms_2d0e007e7136de11d5f29fce7a53dae219a51458@existiert.net"
	}
	settings["RAFT_PORT"] = "8300"
	settings["RAFT_INTERNAL_RPC_PORT"] = "8301"
	settings["RAFT_JOIN"] = raft_join
	settings["RAFT_BOOTSTRAP_EXPECT"] = strconv.Itoa(d.size)

	// first node
	config1 := copySettings(settings)
	config1["CLUSTER_HOSTNAME"] = "node1"
	config1["CLUSTER_GOSSIP_BIND_PORT"] = "7100"
	config1["CLUSTER_DATA_BIND_PORT"] = "7101"
	eg := errgroup.Group{}
	eg.Go(func() (err error) {
		if cs[0], err = startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule,
			config1, networkName, image, Weaviate1, d.withWeaviateExposeGRPCPort); err != nil {
			return errors.Wrapf(err, "start %s", Weaviate1)
		}
		return nil
	})

	if size > 1 {
		config2 := copySettings(settings)
		config2["CLUSTER_HOSTNAME"] = "node2"
		config2["CLUSTER_GOSSIP_BIND_PORT"] = "7102"
		config2["CLUSTER_DATA_BIND_PORT"] = "7103"
		config2["CLUSTER_JOIN"] = fmt.Sprintf("%s:7100", Weaviate1)
		eg.Go(func() (err error) {
			time.Sleep(time.Second * 3)
			cs[1], err = startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule,
				config2, networkName, image, Weaviate2, d.withWeaviateExposeGRPCPort)
			if err != nil {
				return errors.Wrapf(err, "start %s", Weaviate2)
			}
			return nil
		})
	}

	if size > 2 {
		config3 := copySettings(settings)
		config3["CLUSTER_HOSTNAME"] = "node3"
		config3["CLUSTER_GOSSIP_BIND_PORT"] = "7104"
		config3["CLUSTER_DATA_BIND_PORT"] = "7105"
		config3["CLUSTER_JOIN"] = fmt.Sprintf("%s:7100", Weaviate1)
		eg.Go(func() (err error) {
			time.Sleep(time.Second * 3)
			if cs[2], err = startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule,
				config3, networkName, image, Weaviate3, d.withWeaviateExposeGRPCPort); err != nil {
				return errors.Wrapf(err, "start %s", Weaviate3)
			}
			return nil
		})
	}

	return cs, eg.Wait()
}

func copySettings(s map[string]string) map[string]string {
	copy := make(map[string]string, len(s))
	for k, v := range s {
		copy[k] = v
	}
	return copy
}
