package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	Origin            string
	DatasetConfigPath string
	BatchSize         int
	QueriesCount      int
)

const (
	DefaultOrigin            = "http://localhost:8080"
	DefaultDatasetConfigPath = "datasets.yml"
	DefaultBatchSize         = 100
	DefaultQueriesCount      = -1
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
	rootCmd.PersistentFlags().StringVarP(&Origin, "origin", "o", DefaultOrigin, "origin (schem + host + port) where weaviate is running")
	rootCmd.PersistentFlags().StringVar(&DatasetConfigPath, "dataset-config", DefaultDatasetConfigPath, "path to dataset config file")
}
