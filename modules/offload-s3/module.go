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
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/parallel"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
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

var app = &cli.App{
	Name:                 "weaviate-s5cmd",
	Usage:                "weaviate fast S3 and local filesystem execution tool",
	EnableBashCompletion: true,
	Commands:             command.Commands(),
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "numworkers",
			Value: 256,
			Usage: "number of workers execute operation on each object",
		},
		&cli.IntFlag{
			Name:    "retry-count",
			Aliases: []string{"r"},
			Value:   10,
			Usage:   "number of times that a request will be retried for failures",
		},
		&cli.StringFlag{
			Name:    "endpoint-url",
			Usage:   "override default S3 host for custom services",
			EnvVars: []string{"S3_ENDPOINT_URL"},
		},
		&cli.BoolFlag{
			Name:  "no-verify-ssl",
			Usage: "disable SSL certificate verification",
		},
	},
	Before: func(c *cli.Context) error {
		retryCount := c.Int("retry-count")
		workerCount := c.Int("numworkers")
		printJSON := c.Bool("json")
		logLevel := c.String("log")
		isStat := c.Bool("stat")
		endpointURL := c.String("endpoint-url")

		log.Init(logLevel, printJSON)
		parallel.Init(workerCount)

		if retryCount < 0 {
			err := fmt.Errorf("retry count cannot be a negative value")
			return err
		}
		if c.Bool("no-sign-request") && c.String("profile") != "" {
			err := fmt.Errorf(`"no-sign-request" and "profile" flags cannot be used together`)
			return err
		}
		if c.Bool("no-sign-request") && c.String("credentials-file") != "" {
			err := fmt.Errorf(`"no-sign-request" and "credentials-file" flags cannot be used together`)
			return err
		}

		if isStat {
			stat.InitStat()
		}

		if endpointURL != "" {
			if !strings.HasPrefix(endpointURL, "http") {
				err := fmt.Errorf(`bad value for --endpoint-url %v: scheme is missing. Must be of the form http://<hostname>/ or https://<hostname>/`, endpointURL)
				return err
			}
		}

		return nil
	},
	Action: func(c *cli.Context) error {
		if c.Bool("install-completion") {
			return nil
		}
		args := c.Args()
		if args.Present() {
			cli.ShowCommandHelp(c, args.First())
			return cli.Exit("", 1)
		}

		return cli.ShowAppHelp(c)
	},
	After: func(c *cli.Context) error {
		if c.Bool("stat") && len(stat.Statistics()) > 0 {
			log.Stat(stat.Statistics())
		}
		return nil
	},
}

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
	err := m.create(ctx)
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		return fmt.Errorf("can't create offload bucket %w", err)
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

	return app.RunContext(ctx, cmd)
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

	return app.RunContext(ctx, cmd)
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
	return app.RunContext(ctx, cmd)
}
