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
	"time"

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
	timeout     = "OFFLOAD_TIMEOUT"
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
	timeout     time.Duration
	app         *cli.App
}

func New() *Module {

	return &Module{
		Endpoint:    "",
		Bucket:      "weaviate-offload",
		Concurrency: 25,
		DataPath:    config.DefaultPersistenceDataPath,
		timeout:     10 * time.Second,
		// we use custom cli app to avoid some bugs in underlying dependencies
		// specially with .After implementation.
		app: &cli.App{
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
		},
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

	eTimeout := os.Getenv(timeout)
	if eTimeout != "" {
		timeoutN, err := time.ParseDuration(fmt.Sprintf("%ss", eTimeout))
		if err != nil {
			return err
		}
		m.timeout = time.Duration(timeoutN.Seconds()) * time.Second
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
		return fmt.Errorf("can't create offload bucket %s %w", m.Endpoint, err)
	}

	m.logger.WithFields(logrus.Fields{
		concurrency:             m.Concurrency,
		timeout:                 m.timeout,
		endpoint:                m.Endpoint,
		bucket:                  m.Bucket,
		"PERSISTENCE_DATA_PATH": m.DataPath,
	}).Info("offload module loaded")
	return nil
}

func (m *Module) RootHandler() http.Handler {
	return nil
}

func (m *Module) create(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	cmd := []string{
		fmt.Sprintf("--endpoint-url=%s", m.Endpoint),
		"mb",
		fmt.Sprintf("s3://%s", m.Bucket),
	}

	return m.app.RunContext(ctx, cmd)
}

// Upload uploads the content of a shard assigned to specific node to
// cloud provider (S3, Azure Blob storage, Google cloud storage)
// {cloud_provider}://{configured_bucket}/{className}/{shardName}/{nodeName}/{shard content}
func (m *Module) Upload(ctx context.Context, className, shardName, nodeName string) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	cmd := []string{
		fmt.Sprintf("--endpoint-url=%s", m.Endpoint),
		"cp",
		fmt.Sprintf("--concurrency=%s", fmt.Sprintf("%d", m.Concurrency)),
		fmt.Sprintf("%s/%s/%s/*", m.DataPath, strings.ToLower(className), shardName),
		fmt.Sprintf("s3://%s/%s/%s/%s/", m.Bucket, strings.ToLower(className), shardName, nodeName),
	}

	return m.app.RunContext(ctx, cmd)
}

// Download downloads the content of a shard to desired node from
// cloud provider (S3, Azure Blob storage, Google cloud storage)
// {dataPath}/{className}/{shardName}/{content}
func (m *Module) Download(ctx context.Context, className, shardName, nodeName string) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	cmd := []string{
		fmt.Sprintf("--endpoint-url=%s", m.Endpoint),
		"cp",
		fmt.Sprintf("--concurrency=%s", fmt.Sprintf("%d", m.Concurrency)),
		fmt.Sprintf("s3://%s/%s/%s/%s/*", m.Bucket, strings.ToLower(className), shardName, nodeName),
		fmt.Sprintf("%s/%s/%s/", m.DataPath, strings.ToLower(className), shardName),
	}
	return m.app.RunContext(ctx, cmd)
}

// Delete deletes content of a shard assigned to specific node in
// cloud provider (S3, Azure Blob storage, Google cloud storage)
// {cloud_provider}://{configured_bucket}/{className}/{shardName}/{nodeName}/{shard content}
func (m *Module) Delete(ctx context.Context, className, shardName, nodeName string) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	cmd := []string{
		fmt.Sprintf("--endpoint-url=%s", m.Endpoint),
		"rm",
		fmt.Sprintf("s3://%s/%s/%s/%s/*", m.Bucket, strings.ToLower(className), shardName, nodeName),
	}
	err := m.app.RunContext(ctx, cmd)
	if err != nil && !strings.Contains(err.Error(), "no object found") {
		return err
	}
	return nil
}
