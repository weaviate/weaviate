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

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	Origin                 string
	DatasetConfigPath      string
	BatchSize              int
	QueriesCount           int
	MultiplyProperties     int
	FilterObjectPercentage int
	Alpha                  float32
	Ranking                string
	Vectorizer             bool
)

const (
	DefaultOrigin                 = "http://localhost:8080"
	DefaultDatasetConfigPath      = "datasets.yml"
	DefaultBatchSize              = 100
	DefaultQueriesCount           = -1
	DefaultMultiplyProperties     = 1
	DefaultFilterObjectPercentage = 0
	DefaultAlpha                  = 0
	DefaultRanking                = "ranked_fusion"
	DefaultVectorizer             = false
)

var rootCmd = &cobra.Command{
	Use:   "benchmarker",
	Short: "benchmarker is a simple tool to obtain bm25 speed benchmarks",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Run --help to see usage instructions.\n")
		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&Origin, "origin", "o", DefaultOrigin, "origin (schema + host + port) where weaviate is running")
	rootCmd.PersistentFlags().StringVar(&DatasetConfigPath, "dataset-config", DefaultDatasetConfigPath, "path to dataset config file")
}
