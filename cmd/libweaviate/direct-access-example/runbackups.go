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

package main

// This example shows how to start weaviate and get the appState, which can be used for scripting and direct control

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

func main() {
	dataPath := flag.String("data-path", "./data", "path to the data directory")
	origin := flag.String("origin", "http://localhost:8080", "listen address")
	// clusterHostname := flag.String("cluster-hostname", "node1", "cluster hostname")
	defaultVectoriser := flag.String("default-vectoriser", "none", "default vectoriser")

	flag.Parse()
	// set environment variables
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("LOG_FORMAT", "text")
	os.Setenv("PROMETHEUS_MONITORING_ENABLED", "true")
	os.Setenv("GO_BLOCK_PROFILE_RATE", "20")
	os.Setenv("GO_MUTEX_PROFILE_FRACTION", "20")
	os.Setenv("PERSISTENCE_DATA_PATH", *dataPath)
	os.Setenv("ORIGIN", *origin)
	os.Setenv("QUERY_DEFAULTS_LIMIT", "20")
	os.Setenv("QUERY_MAXIMUM_RESULTS", "10000")
	// os.Setenv("CLUSTER_HOSTNAME", *clusterHostname)
	os.Setenv("TRACK_VECTOR_DIMENSIONS", "true")
	// os.Setenv("CLUSTER_GOSSIP_BIND_PORT", "7100")
	// os.Setenv("CLUSTER_DATA_BIND_PORT", "7101")
	os.Setenv("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED", "true")
	os.Setenv("DEFAULT_VECTORIZER_MODULE", *defaultVectoriser)
	// os.Setenv("CLUSTER_IN_LOCALHOST", "true")

	config := config.GetConfigOptionGroup()
	appState := rest.MakeAppState(context.Background(), config)
	backupScheduler := rest.StartBackupScheduler(appState)
	fmt.Printf("BackupScheduler: %+v\n", backupScheduler)

	backman := appState.BackupManager
	if backman != nil {
		fmt.Printf("BackupManager: %+v\n", backman)
		backupper := backman.GetBackUpper()
		bp := backman.GetBackends()
		backend, err := bp.BackupBackend("filesystem")
		if err != nil {
			fmt.Printf("Error: %+v\n", err)
		}
		if backend == nil {
			fmt.Printf("Backend is nil\n")
			os.Exit(1)
		}
		if backupper != nil {
			fmt.Printf("Backupper: %+v\n", backupper)
			ns := backup.NodeStore{
				backup.ObjectStore{
					backend,
					"test",
					"", "",
				},
			}
			meta, err := backupper.Backup(context.Background(), ns, "test", []string{"Page"}, "", "")
			if err != nil {
				fmt.Printf("Error: %+v\n", err)
			}
			fmt.Printf("BackupMeta: %+v\n", meta)

			status, err := backupper.Status(context.Background(), "filesystem", "test")
			if err != nil {
				fmt.Printf("Error: %+v\n", err)
			}

			fmt.Printf("BackupStatus: %+v\n", status)
			for {

				status, err = backupper.Status(context.Background(), "filesystem", "test")
				if err != nil {
					fmt.Printf("Error: %+v\n", err)
				}
				if status.Status != nil {
					fmt.Printf("BackupStatus %v: %+v\n", *status.Status, status)
				} else {
					fmt.Printf("BackupStatus: %+v\n", status)
					break
				}
				backupper.OnCommit(context.Background(), &backup.StatusRequest{"create", "test", "filesystem", "", ""})
			}

		}
	}

	/* TODO dump vector repo
	vectorRepo := appState.DB
	fmt.Printf("vectorRepo: %+v\n", vectorRepo)
	*/
	os.Exit(0)
}
