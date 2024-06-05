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

package modsloads3

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/peak/s5cmd/v2/command"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	Name        = "offload-s3"
	s3Endpoint  = "OFFLOAD_S3_ENDPOINT"
	s3Bucket    = "OFFLOAD_S3_BUCKET"
	concurrency = "OFFLOAD_S3_CONCURRENCY"
)

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
)

type Module struct {
	Endpoint    string
	Bucket      string
	Concurrency int
	DataPath    string
	logger      logrus.FieldLogger
}

func New() *Module {
	return &Module{
		Endpoint:    "",
		Bucket:      "weaviate-offload",
		Concurrency: 25,
		DataPath:    config.DefaultPersistenceDataPath,
	}
}

func (m *Module) Name() string {
	return Name
}

func (m *Module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Offload
}

func (m *Module) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()

	if path := os.Getenv("PERSISTENCE_DATA_PATH"); path != "" {
		m.DataPath = path
	}

	bucket := os.Getenv(s3Bucket)
	if bucket != "" {
		m.Bucket = bucket
	}

	endpoint := os.Getenv(s3Endpoint)
	if endpoint != "" {
		m.Endpoint = endpoint
	}

	concc := os.Getenv(concurrency)
	if concc != "" {
		conccN, err := strconv.Atoi(concc)
		if err != nil {
			return err
		}
		m.Concurrency = conccN
	}

	// create offloading bucket
	// TODO check instead of create
	err := m.create(ctx)
	// todo proper error handling
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		return err
	}
	return nil
}

func (m *Module) RootHandler() http.Handler {
	return nil
}

func (m *Module) create(ctx context.Context) error {
	cmd := []string{
		fmt.Sprintf("--endpoint-url=%s", m.Endpoint),
		"mb",
		fmt.Sprintf("s3://%s", m.Bucket),
	}

	return command.Main(ctx, cmd)
}

// Upload uploads the context of a shard to s3
// upload path is
// s3://{}
func (m *Module) Upload(ctx context.Context, className, shardName string) error {
	cmd := []string{
		fmt.Sprintf("--endpoint-url=%s", m.Endpoint),
		"cp",
		fmt.Sprintf("--concurrency=%s", fmt.Sprintf("%d", m.Concurrency)),
		fmt.Sprintf("%s/%s/%s/*", m.DataPath, className, shardName),
		fmt.Sprintf("s3://%s/%s/%s/%s/", m.Bucket, className, shardName, strings.Split(m.DataPath, "/")[1]),
	}
	err := command.Main(ctx, cmd)
	if err != nil {
		return err
	}

	return os.RemoveAll(fmt.Sprintf("%s/%s/%s", m.DataPath, className, shardName))
}

// Download uploads the context of a shard to s3
// download s3 bucket content
// s3://{}
func (m *Module) Download(ctx context.Context, className, shardName string) error {
	cmd := []string{
		fmt.Sprintf("--endpoint-url=%s", m.Endpoint),
		"cp",
		fmt.Sprintf("--concurrency=%s", fmt.Sprintf("%d", m.Concurrency)),
		fmt.Sprintf("s3://%s/%s/%s/%s/*", m.Bucket, className, shardName, strings.Split(m.DataPath, "/")[1]),
		fmt.Sprintf("%s/%s/%s/", m.DataPath, className, shardName),
	}
	return command.Main(ctx, cmd)
}
