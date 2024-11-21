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

package docker

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	tescontainersnetwork "github.com/testcontainers/testcontainers-go/network"
	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	modstgfilesystem "github.com/weaviate/weaviate/modules/backup-filesystem"
	modstggcs "github.com/weaviate/weaviate/modules/backup-gcs"
	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"
	modgenerativeanyscale "github.com/weaviate/weaviate/modules/generative-anyscale"
	modgenerativeaws "github.com/weaviate/weaviate/modules/generative-aws"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativegoogle "github.com/weaviate/weaviate/modules/generative-google"
	modgenerativeollama "github.com/weaviate/weaviate/modules/generative-ollama"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modmulti2veccohere "github.com/weaviate/weaviate/modules/multi2vec-cohere"
	modmulti2vecgoogle "github.com/weaviate/weaviate/modules/multi2vec-google"
	modmulti2vecjinaai "github.com/weaviate/weaviate/modules/multi2vec-jinaai"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	modrerankervoyageai "github.com/weaviate/weaviate/modules/reranker-voyageai"
	modaws "github.com/weaviate/weaviate/modules/text2vec-aws"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	modjinaai "github.com/weaviate/weaviate/modules/text2vec-jinaai"
	modollama "github.com/weaviate/weaviate/modules/text2vec-ollama"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modvoyageai "github.com/weaviate/weaviate/modules/text2vec-voyageai"
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
	// envTestMulti2VecBindImage adds ability to pass a custom BIND image to module tests
	envTestMulti2VecBindImage = "TEST_MULTI2VEC_BIND_IMAGE"
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
	withGoogleApiKey              string
	withBind                      bool
	withImg2Vec                   bool
	withRerankerTransformers      bool
	withOllamaVectorizer          bool
	withOllamaGenerative          bool
	weaviateEnvs                  map[string]string
}

func New() *Compose {
	return &Compose{enableModules: []string{}, weaviateEnvs: make(map[string]string)}
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

func (d *Compose) WithText2VecOllama() *Compose {
	d.withOllamaVectorizer = true
	d.enableModules = append(d.enableModules, modollama.Name)
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

func (d *Compose) WithMulti2VecGoogle(apiKey string) *Compose {
	d.withGoogleApiKey = apiKey
	d.enableModules = append(d.enableModules, modmulti2vecgoogle.Name)
	return d
}

func (d *Compose) WithMulti2VecCohere(apiKey string) *Compose {
	d.weaviateEnvs["COHERE_APIKEY"] = apiKey
	d.enableModules = append(d.enableModules, modmulti2veccohere.Name)
	return d
}

func (d *Compose) WithMulti2VecJinaAI(apiKey string) *Compose {
	d.weaviateEnvs["JINAAI_APIKEY"] = apiKey
	d.enableModules = append(d.enableModules, modmulti2vecjinaai.Name)
	return d
}

func (d *Compose) WithMulti2VecBind() *Compose {
	d.withBind = true
	d.enableModules = append(d.enableModules, Multi2VecBind)
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

func (d *Compose) WithText2VecVoyageAI() *Compose {
	d.enableModules = append(d.enableModules, modvoyageai.Name)
	return d
}

func (d *Compose) WithText2VecGoogle(apiKey string) *Compose {
	d.withGoogleApiKey = apiKey
	d.enableModules = append(d.enableModules, modgoogle.Name)
	return d
}

func (d *Compose) WithText2VecAWS(accessKey, secretKey, sessionToken string) *Compose {
	d.weaviateEnvs["AWS_ACCESS_KEY"] = accessKey
	d.weaviateEnvs["AWS_SECRET_KEY"] = secretKey
	d.weaviateEnvs["AWS_SESSION_TOKEN"] = sessionToken
	d.enableModules = append(d.enableModules, modaws.Name)
	return d
}

func (d *Compose) WithText2VecHuggingFace() *Compose {
	d.enableModules = append(d.enableModules, modhuggingface.Name)
	return d
}

func (d *Compose) WithText2VecJinaAI(apiKey string) *Compose {
	d.weaviateEnvs["JINAAI_APIKEY"] = apiKey
	d.enableModules = append(d.enableModules, modjinaai.Name)
	return d
}

func (d *Compose) WithGenerativeOpenAI() *Compose {
	d.enableModules = append(d.enableModules, modgenerativeopenai.Name)
	return d
}

func (d *Compose) WithGenerativeAWS(accessKey, secretKey, sessionToken string) *Compose {
	d.weaviateEnvs["AWS_ACCESS_KEY"] = accessKey
	d.weaviateEnvs["AWS_SECRET_KEY"] = secretKey
	d.weaviateEnvs["AWS_SESSION_TOKEN"] = sessionToken
	d.enableModules = append(d.enableModules, modgenerativeaws.Name)
	return d
}

func (d *Compose) WithGenerativeCohere() *Compose {
	d.enableModules = append(d.enableModules, modgenerativecohere.Name)
	return d
}

func (d *Compose) WithGenerativeGoogle(apiKey string) *Compose {
	d.withGoogleApiKey = apiKey
	d.enableModules = append(d.enableModules, modgenerativegoogle.Name)
	return d
}

func (d *Compose) WithGenerativeAnyscale() *Compose {
	d.enableModules = append(d.enableModules, modgenerativeanyscale.Name)
	return d
}

func (d *Compose) WithGenerativeOllama() *Compose {
	d.withOllamaGenerative = true
	d.enableModules = append(d.enableModules, modgenerativeollama.Name)
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

func (d *Compose) WithRerankerVoyageAI() *Compose {
	d.enableModules = append(d.enableModules, modrerankervoyageai.Name)
	return d
}

func (d *Compose) WithRerankerTransformers() *Compose {
	d.withRerankerTransformers = true
	d.enableModules = append(d.enableModules, RerankerTransformers)
	return d
}

func (d *Compose) WithOllamaVectorizer() *Compose {
	d.withOllamaVectorizer = true
	return d
}

func (d *Compose) WithOllamaGenerative() *Compose {
	d.withOllamaGenerative = true
	return d
}

func (d *Compose) WithWeaviate() *Compose {
	return d.With1NodeCluster()
}

func (d *Compose) WithWeaviateWithGRPC() *Compose {
	d.With1NodeCluster()
	d.withWeaviateExposeGRPCPort = true
	return d
}

func (d *Compose) WithWeaviateCluster() *Compose {
	return d.With3NodeCluster()
}

func (d *Compose) WithWeaviateClusterWithGRPC() *Compose {
	d.With3NodeCluster()
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
	d.withWeaviateAuth = true
	return d.With1NodeCluster()
}

func (d *Compose) WithWeaviateEnv(name, value string) *Compose {
	d.weaviateEnvs[name] = value
	return d
}

func (d *Compose) Start(ctx context.Context) (*DockerCompose, error) {
	d.weaviateEnvs["DISABLE_TELEMETRY"] = "true"
	network, err := tescontainersnetwork.New(
		ctx,
		tescontainersnetwork.WithAttachable(),
	)
	networkName := network.Name
	if err != nil {
		return nil, errors.Wrapf(err, "network: %s", networkName)
	}
	envSettings := make(map[string]string)
	envSettings["network"] = networkName
	envSettings["DISABLE_TELEMETRY"] = "true"
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
	if d.withOllamaVectorizer {
		container, err := startOllamaVectorizer(ctx, networkName)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", OllamaVectorizer)
		}
		for k, v := range container.envSettings {
			envSettings[k] = v
		}
		containers = append(containers, container)
	}
	if d.withOllamaGenerative {
		container, err := startOllamaGenerative(ctx, networkName)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", OllamaGenerative)
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
	if d.withGoogleApiKey != "" {
		envSettings["GOOGLE_APIKEY"] = d.withGoogleApiKey
	}
	if d.withBind {
		image := os.Getenv(envTestMulti2VecBindImage)
		container, err := startM2VBind(ctx, networkName, image)
		if err != nil {
			return nil, errors.Wrapf(err, "start %s", Multi2VecBind)
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

	if d.withSecondWeaviate {
		image := os.Getenv(envTestWeaviateImage)
		hostname := SecondWeaviate
		secondWeaviateSettings := envSettings
		// Ensure second weaviate doesn't get cluster settings from the first cluster if any.
		delete(secondWeaviateSettings, "CLUSTER_HOSTNAME")
		delete(secondWeaviateSettings, "CLUSTER_GOSSIP_BIND_PORT")
		delete(secondWeaviateSettings, "CLUSTER_DATA_BIND_PORT")
		delete(secondWeaviateSettings, "CLUSTER_JOIN")
		for k, v := range d.weaviateEnvs {
			envSettings[k] = v
		}
		delete(secondWeaviateSettings, "RAFT_PORT")
		delete(secondWeaviateSettings, "RAFT_INTERNAL_PORT")
		delete(secondWeaviateSettings, "RAFT_JOIN")
		container, err := startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule, envSettings, networkName, image, hostname, d.withWeaviateExposeGRPCPort, "/v1/.well-known/ready")
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

func (d *Compose) With3NodeCluster() *Compose {
	d.withWeaviateCluster = true
	d.size = 3
	return d
}

func (d *Compose) startCluster(ctx context.Context, size int, settings map[string]string) ([]*DockerContainer, error) {
	if size == 0 || size > 3 {
		return nil, nil
	}
	for k, v := range d.weaviateEnvs {
		settings[k] = v
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
	settings["DISABLE_TELEMETRY"] = "true"
	if d.withWeaviateBasicAuth {
		settings["CLUSTER_BASIC_AUTH_USERNAME"] = d.withWeaviateBasicAuthUsername
		settings["CLUSTER_BASIC_AUTH_PASSWORD"] = d.withWeaviateBasicAuthPassword
	}
	if d.withWeaviateAuth {
		settings["AUTHENTICATION_OIDC_ENABLED"] = "true"
		settings["AUTHENTICATION_OIDC_CLIENT_ID"] = "wcs"
		settings["AUTHENTICATION_OIDC_ISSUER"] = "https://auth.wcs.api.weaviate.io/auth/realms/SeMI"
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
	wellKnownEndpointFunc := func(hostname string) string {
		if slices.Contains(strings.Split(settings["MAINTENANCE_NODES"], ","), hostname) {
			return "/v1/.well-known/live"
		}
		return "/v1/.well-known/ready"
	}
	eg.Go(func() (err error) {
		cs[0], err = startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule,
			config1, networkName, image, Weaviate1, d.withWeaviateExposeGRPCPort, wellKnownEndpointFunc("node1"))
		if err != nil {
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
				config2, networkName, image, Weaviate2, d.withWeaviateExposeGRPCPort, wellKnownEndpointFunc("node2"))
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
			cs[2], err = startWeaviate(ctx, d.enableModules, d.defaultVectorizerModule,
				config3, networkName, image, Weaviate3, d.withWeaviateExposeGRPCPort, wellKnownEndpointFunc("node3"))
			if err != nil {
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
