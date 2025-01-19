//go:build ignore
// +build ignore

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/run"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func main() {
	// logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.TextFormatter{})

	// flags
	var (
		target            string
		runtimeConfig     string
		dataPath          string
		backupDestination string
		backupEnabled     bool
	)
	flag.StringVar(&target, "target", "all", "target running mode (all, api-server, backup-server, retention-server")
	flag.StringVar(&runtimeConfig, "runtime-config", "rconfig.yaml", "runtime config that will be reloaded every second")
	flag.StringVar(&dataPath, "path", "data", "data path for segment files")
	flag.StringVar(&backupDestination, "backup-dst", "backup", "destination to copy the backups to")
	flag.BoolVar(&backupEnabled, "backup-enabled", false, "whether to enable backup")
	flag.Parse()

	// run group lifecycle
	var g run.Group
	ctx, cancel := context.WithCancel(context.Background())

	if target == "api-server" || target == "all" {
		// api-server

		a := ApiServer{
			logger:            logger,
			dataPath:          dataPath,
			bc:                lsmkv.NewBucketCreator(),
			skipCompactMarker: backupEnabled,
		}
		g.Add(func() error {
			return a.Run(ctx)
		}, func(err error) {
			logger.Info("api server stopped")
			cancel()
		})
	}
	if target == "backup-server" || target == "all" {
		// backup-server
		a := BackupServer{
			logger: logger,
		}

		// // cron
		// g.Add(func() error {
		// 	return a.RunBackupCron(ctx)
		// }, func(err error) {
		// 	logger.Info("backup server stopped")
		// 	cancel()
		// })

		// http server
		listener, err := net.Listen("tcp", "0.0.0.0:7070")
		g.Add(func() error {
			h := a.HTTPHandler(ctx)
			if err != nil {
				logger.Error("failed to create http listener")
				return err
			}
			return http.Serve(listener, h)
		}, func(err error) {
			logger.Info("backup server stopped")
			cancel()
			listener.Close()
		})

	}
	if target == "retention-server" || target == "all" {
		// retention-server
		a := RetentionServer{
			logger: logger,
		}
		g.Add(func() error {
			return a.Run(ctx)
		}, func(err error) {
			logger.Info("retention server stopped")
			cancel()
		})
	}

	// Run all the actors
	if err := g.Run(); err != nil {
		logger.Info("stopping the server")
		os.Exit(1)
	}
}

// Think of core weaviate api server. The whole purpose of `ApiServer` here is to
// generate segment files without compaction (same assumption as Eitenie POC)
type ApiServer struct {
	logger            logrus.FieldLogger
	dataPath          string
	bc                lsmkv.BucketCreator
	skipCompactMarker bool
}

func (a *ApiServer) Run(ctx context.Context) error {
	a.logger.Info("running api-server")
	flushCallbacks := cyclemanager.NewCallbackGroup("flush", a.logger, 1)
	flushCycle := cyclemanager.NewManager(cyclemanager.MemtableFlushCycleTicker(), flushCallbacks.CycleCallback, a.logger)
	flushCycle.Start()

	compactionCallbacks := cyclemanager.NewCallbackGroup("compaction", a.logger, 1)
	compactionCycle := cyclemanager.NewManager(cyclemanager.CompactionCycleTicker(), compactionCallbacks.CycleCallback, a.logger)
	compactionCycle.Start()

	h, m, s := time.Now().Clock()

	bucket, err := a.bc.NewBucket(ctx, filepath.Join(a.dataPath, "my-bucket"), "", a.logger, nil,
		compactionCallbacks, flushCallbacks,
		lsmkv.WithPread(true),
	)
	if err != nil {
		panic(err)
	}

	defer bucket.Shutdown(context.Background())

	// create phase segement files every 30s
	tk := time.NewTicker(30 * time.Second)
	phase := 0
	for {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("phase-%02d-key-%03d", phase, i))
			value := []byte(fmt.Sprintf("written %02d:%02d:%02d", h, m, s))
			err := bucket.Put(key, value)
			if err != nil {
				panic(err)
			}
		}

		err, path := bucket.FlushAndSwitchX()
		if err != nil {
			panic(err)
		}
		if a.skipCompactMarker {
			if _, err := os.Create(fmt.Sprintf("%s.%s", path, "skip-compact")); err != nil {
				panic(err)
			}
		}

		<-tk.C
		phase++
	}
}

// BackupServer handles copying segment files from `src` to `dst`
// This can be run as different container. But needs to share the pod because
// `src` is same filesystem where segments are written by `ApiServer` and need to be
// shared between `ApiServer` and `BackupServer`
type BackupServer struct {
	logger logrus.FieldLogger
	policy func() *BackupPolicy
}

// Responsible for triggering backup based on given backup policy
func (a *BackupServer) RunBackupCron(ctx context.Context, policy BackupPolicy) error {
	a.logger.Info("running backup cron server")
	// check for runtime config changes every 10s
	for {
		// t := time.NewTicker(a.policy().Every)

		// time.Sleep(10 * time.Second)
	}
	return nil
}

type BackupPolicy struct {
	Every time.Duration `yaml:"every"`
}

// Responsible for user facing /v1/backup and /v1/restore.
func (a *BackupServer) HTTPHandler(ctx context.Context) http.Handler {
	a.logger.Info("running backup http server")
	for {
	}
	return nil
}

// does the heavy job of backup.
func (a *BackupServer) backup(ctx context.Context) error {
	a.logger.Info("backup done")
	return nil
}

// does the heavy job of restore.
func (a *BackupServer) restore(ctx context.Context) error {
	a.logger.Info("restore done")
	return nil
}

// RetentionServer is takes care of segment files copied to `dst` by the backupserver.
// it's responsibilities are
// 1. How long the segment files lives?
// 2. When to merge these files to be able to restore backup like `hourly`, `weekly` and `monthly` retentions.
type RetentionServer struct {
	logger logrus.FieldLogger
}

func (a *RetentionServer) Run(ctx context.Context) error {
	a.logger.Info("running retention-server")
	for {
	}
	return nil
}

// Runtime config manager
type RuntimeConfigManager struct {
	// mu protects the policy and keep it upto date with
	// runtime overrides
	mu     sync.Mutex // can be RWLock.
	config RuntimeConfig
}

type RuntimeConfig struct{}
