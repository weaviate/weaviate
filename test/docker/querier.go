package docker

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	Querier = "querier"
)

func startQuerier(ctx context.Context,
	enableModules []string, defaultVectorizerModule string,
	extraEnvSettings map[string]string, networkName string,
	querierImage, hostname string, exposeGRPCPort bool,
	wellKnownEndpoint string,
) (*DockerContainer, error) {
	fromDockerFile := testcontainers.FromDockerfile{}
	if len(querierImage) == 0 {
		path, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		getContextPath := func(path string) string {
			if strings.Contains(path, "test/acceptance_with_go_client") {
				return path[:strings.Index(path, "/test/acceptance_with_go_client")]
			}
			if strings.Contains(path, "test/acceptance") {
				return path[:strings.Index(path, "/test/acceptance")]
			}
			return path[:strings.Index(path, "/test/modules")]
		}
		targetArch := runtime.GOARCH
		gitHashBytes, err := exec.Command("git", "rev-parse", "--short", "HEAD").CombinedOutput()
		if err != nil {
			return nil, err
		}
		gitHash := strings.ReplaceAll(string(gitHashBytes), "\n", "")
		contextPath := getContextPath(path)
		fromDockerFile = testcontainers.FromDockerfile{
			Context:    contextPath,
			Dockerfile: "Dockerfile",
			BuildArgs: map[string]*string{
				"TARGETARCH":   &targetArch,
				"GIT_REVISION": &gitHash,
			},
			PrintBuildLog: true,
			KeepImage:     false,
			BuildOptionsModifier: func(buildOptions *types.ImageBuildOptions) {
				buildOptions.Target = "weaviate_experimental"
			},
		}
	}
	containerName := Querier
	if hostname != "" {
		containerName = hostname
	}
	env := map[string]string{
		"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
		"LOG_LEVEL":                     "debug",
		"QUERY_DEFAULTS_LIMIT":          "20",
		"PERSISTENCE_DATA_PATH":         "./data",
		"DEFAULT_VECTORIZER_MODULE":     "none",
		"FAST_FAILURE_DETECTION":        "true",
		"OFFLOAD_S3_BUCKET_AUTO_CREATE": "true",
		"OFFLOAD_S3_ENDPOINT":           "http://test-minio:9000",
		"AWS_SECRET_KEY":                "aws_secret_key",
		"AWS_ACCESS_KEY":                "aws_access_key",
	}
	if len(enableModules) > 0 {
		env["ENABLE_MODULES"] = strings.Join(enableModules, ",")
	}
	if len(defaultVectorizerModule) > 0 {
		env["DEFAULT_VECTORIZER_MODULE"] = defaultVectorizerModule
	}
	for key, value := range extraEnvSettings {
		env[key] = value
	}
	exposedPorts := []string{}
	waitStrategies := []wait.Strategy{}
	grpcPort := nat.Port("7071/tcp")
	if exposeGRPCPort {
		exposedPorts = append(exposedPorts, "7071/tcp")
		waitStrategies = append(waitStrategies, wait.ForListeningPort(grpcPort))
	}
	req := testcontainers.ContainerRequest{
		FromDockerfile: fromDockerFile,
		Image:          querierImage,
		Hostname:       containerName,
		Name:           containerName,
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {containerName},
		},
		ExposedPorts: exposedPorts,
		Env:          env,
		Cmd: []string{
			"--target=querier",
			"--query.schema.addr=http://weaviate:8080",
			"--query.s3.endpoint=http://test-minio:9000",
		},
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
			{
				// Use wait strategies as part of the lifecycle hooks as this gets propagated to the underlying container,
				// which survives stop/start commands
				PostStarts: []testcontainers.ContainerHook{
					func(ctx context.Context, container testcontainers.Container) error {
						for _, waitStrategy := range waitStrategies {
							ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
							defer cancel()

							if err := waitStrategy.WaitUntilReady(ctx, container); err != nil {
								return err
							}
						}
						return nil
					},
				},
			},
		},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
	if err != nil {
		return nil, err
	}
	endpoints := make(map[EndpointName]endpoint)
	if exposeGRPCPort {
		grpcUri, err := c.PortEndpoint(ctx, grpcPort, "")
		if err != nil {
			return nil, err
		}
		endpoints[GRPC] = endpoint{grpcPort, grpcUri}
	}
	return &DockerContainer{
		name:        containerName,
		endpoints:   endpoints,
		container:   c,
		envSettings: nil,
	}, nil
}
