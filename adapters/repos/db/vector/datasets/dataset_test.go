package datasets

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLoadDataset(t *testing.T) {
	logger := logrus.New()
	hf := NewHubDataset("trengrj/ann-datasets", "dbpedia-openai-100k", logger)
	hf.LoadTrainingData()
}
