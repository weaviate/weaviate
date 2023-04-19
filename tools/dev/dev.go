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

package main

import (
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/jessevdk/go-flags"
)

func main() {
	var opts struct {
		Node                   int    `long:"node" default:"1"`
		Delete                 bool   `long:"delete"`
		Restart                bool   `long:"restart"`
		Keycloak               bool   `long:"keycloak"`
		Transformers           string `long:"transformers" choice:"t2v-pq" choice:"t2v" choice:"ctx"`
		Contextionary          bool   `long:"contextionary"`
		QNA                    bool   `long:"qna"`
		Sum                    bool   `long:"sum"`
		Image                  bool   `long:"image"`
		Ner                    bool   `long:"ner"`
		TextSpellcheck         bool   `long:"spellcheck"`
		Multi2vecClip          bool   `long:"clip"`
		Prometheus             bool   `long:"prometheus"`
		S3                     bool   `long:"s3" `
		GCS                    bool   `long:"gcs"`
		Azure                  bool   `long:"azure"`
		LogLevel               string `long:"log-level" default:"debug"`
		LogFormat              string `long:"log-format" default:"text"`
		PrometheusMonitoring   string `long:"prometheus-monitor" default:"false"`
		GoBlockProfileRate     int    `long:"block-rate" default:"20"`
		GoMutexProfileFraction int    `long:"mutex-fraction" default:"20"`
		PersistentDataPath     string `long:"persistent-path" default:"data"`
		Origin                 string `long:"origin" default:"localhost:8080"`
		QueryDefaultLimit      string `long:"query-limit" default:"20"`
		QueryMaxResults        string `long:"query-max" default:"10000"`
	}

	_, err := flags.Parse(&opts)
	if err != nil {
		panic(err)
	}

	var additionalServices []string

	if opts.Keycloak {
		additionalServices = append(additionalServices, "keycloak")
	}

	if opts.Transformers == "t2v-pq" {
		additionalServices = append(additionalServices, "t2v-transformers-passage")
		additionalServices = append(additionalServices, "t2v-transformers-query")
	} else if opts.Transformers == "t2v" {
		additionalServices = append(additionalServices, "t2v-transformers")
	} else {
		additionalServices = append(additionalServices, "contextionary")
	}

	if opts.Contextionary {
		additionalServices = append(additionalServices, "contextionary")
	}
	if opts.QNA {
		additionalServices = append(additionalServices, "qna-transformers")
	}
	if opts.Sum {
		additionalServices = append(additionalServices, "sum-transformers")
	}
	if opts.Image {
		additionalServices = append(additionalServices, "i2v-neural")
	}
	if opts.Multi2vecClip {
		additionalServices = append(additionalServices, "multi2vec-clip")
	}
	if opts.Prometheus {
		additionalServices = append(additionalServices, "prometheus")
		additionalServices = append(additionalServices, "grafana")
	}
	if opts.S3 {
		additionalServices = append(additionalServices, "backup-s3")
	}
	if opts.GCS {
		additionalServices = append(additionalServices, "backup-gcs")
	}
	if opts.Azure {
		additionalServices = append(additionalServices, "backup-azure")
	}

	if opts.Delete {
		execCommand(nil, "rm", "-rf", "data", "data-node2", "connector_state.json", "schema_state.json")
	}

	if opts.Restart {
		execCommand(nil, "docker", "compose", "-f", "docker-compose.yml", "down", "--remove-orphans")
	}

	execCommand(nil, "docker", append([]string{"compose", "-f", "docker-compose.yml", "up", "-d"}, additionalServices...)...)

	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	gitHash, _ := cmd.Output()
	path, _ := os.Getwd()

	defaultEnvArgs := []string{
		"GO111MODULE=on",
		"LOG_LEVEL=" + opts.LogLevel,
		"LOG_FORMAT=" + opts.LogFormat,
		"PROMETHEUS_MONITORING_ENABLED=" + opts.PrometheusMonitoring,
		"GO_BLOCK_PROFILE_RATE=" + strconv.Itoa(opts.GoBlockProfileRate),
		"GO_MUTEX_PROFILE_FRACTION=" + strconv.Itoa(opts.GoMutexProfileFraction),
		"PERSISTENCE_DATA_PATH=./" + opts.PersistentDataPath,
		"ORIGIN=" + opts.Origin,
		"QUERY_DEFAULTS_LIMIT=" + opts.QueryDefaultLimit,
		"QUERY_MAXIMUM_RESULTS=" + opts.QueryMaxResults,
		"TRACK_VECTOR_DIMENSIONS=true",
	}

	weaviatePort := 8080 + opts.Node - 1
	gossipPort := 7100 + (opts.Node-1)*2
	clusterDataBindPort := gossipPort + 1

	envArgs := []string{
		"CONTEXTIONARY_URL=localhost:9999",
		"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true",
		"DEFAULT_VECTORIZER_MODULE=text2vec-contextionary",
		"BACKUP_FILESYSTEM_PATH=" + path + "/backups" + "/node" + strconv.Itoa(opts.Node),
		"ENABLE_MODULES=backup-filesystem,text2vec-contextionary",
		"CLUSTER_GOSSIP_BIND_PORT=" + strconv.Itoa(gossipPort),
		"CLUSTER_DATA_BIND_PORT=" + strconv.Itoa(clusterDataBindPort),
		"CLUSTER_HOSTNAME=node" + strconv.Itoa(opts.Node),
		"CLUSTER_JOIN=localhost:7100",
	}

	execCommand(append(defaultEnvArgs, envArgs...), "go", "run",
		"-ldflags", "-X github.com/weaviate/weaviate/usecases/config.GitHash="+string(strings.Trim(string(gitHash), "\n")),
		"./cmd/weaviate-server",
		"--scheme", "http",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(weaviatePort),
		"--read-timeout", "600s",
		"--write-timeout", "600s",
	)
}

func execCommand(envVars []string, name string, arg ...string) {
	cmd := exec.Command(name, arg...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if envVars != nil {
		cmd.Env = os.Environ()
		for _, env := range envVars {
			cmd.Env = append(cmd.Env, env)
		}
	}

	if err := cmd.Run(); err != nil {
		panic(err)
	}
}
