package lib

import (
	"os"

	"gopkg.in/yaml.v3"
)

type DatasetCfg struct {
	Datasets []Dataset `yaml:"datasets"`
}

type Dataset struct {
	ID      string         `yaml:"id"`
	Path    string         `yaml:"path"`
	Corpus  DatasetCorpus  `yaml:"corpus"`
	Queries DatasetQueries `yaml:"queries"`
}

type DatasetCorpus struct {
	IndexedProperties   []string `yaml:"indexed_properties"`
	UnindexedProperties []string `yaml:"unindexed_properties"`
}

type DatasetQueries struct {
	Property string `yaml:"property"`
}

func ParseDatasetConfig(filename string) (DatasetCfg, error) {
	var config DatasetCfg
	yamlFile, err := os.ReadFile(filename)
	if err != nil {
		return config, err
	}

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return config, err
	}

	return config, nil
}
