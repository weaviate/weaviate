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
	"strings"

	"github.com/jessevdk/go-flags"
)

func main() {
	var opts struct {
		Node           int    `long:"node" default:"1"`
		Delete         bool   `long:"delete"`
		Restart        bool   `long:"restart"`
		Keycloak       bool   `long:"keycloak"`
		Transformers   string `long:"transformers" choice:"t2v-pq" choice:"t2v" choice:"ctx"`
		Contextionary  bool   `long:"contextionary"`
		QNA            bool   `long:"qna"`
		Sum            bool   `long:"sum"`
		Image          bool   `long:"image"`
		Ner            bool   `long:"ner"`
		TextSpellcheck bool   `long:"spellcheck"`
		Multi2vecClip  bool   `long:"clip"`
		Prometheus     bool   `long:"prometheus"`
		S3             bool   `long:"s3" `
		GCS            bool   `long:"gcs"`
		Azure          bool   `long:"azure"`
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
		"LOG_LEVEL=debug",
		"LOG_FORMAT=text",
		"PERSISTENCE_DATA_PATH=./data",
	}

	if opts.Node == 1 {
		envArgs := []string{
			"CONTEXTIONARY_URL=localhost:9999",
			"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true",
			"DEFAULT_VECTORIZER_MODULE=text2vec-openai",
			"BACKUP_FILESYSTEM_PATH=" + path + "/backups",
			"ENABLE_MODULES=text2vec-backup-filesystem,text2vec-openai",
			"CLUSTER_GOSSIP_BIND_PORT=7100",
			"CLUSTER_DATA_BIND_PORT=7101",
		}
		execCommand(append(defaultEnvArgs, envArgs...), "go", "run",
			"-ldflags", "-X github.com/weaviate/weaviate/usecases/config.GitHash="+string(strings.Trim(string(gitHash), "\n")),
			"./cmd/weaviate-server",
			"--scheme", "http",
			"--host", "127.0.0.1",
			"--port", "8080",
			"--read-timeout", "600s",
			"--write-timeout", "600s",
		)
	} else {
	}
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
