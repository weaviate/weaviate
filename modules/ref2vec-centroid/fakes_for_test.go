package modcentroid

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/moduletools"
)

type fakeClassConfig map[string]interface{}

func (cfg fakeClassConfig) Class() map[string]interface{} {
	return cfg
}

func (cfg fakeClassConfig) Property(string) map[string]interface{} {
	return nil
}

func newFakeStorageProvider(t *testing.T) *fakeStorageProvider {
	dirName := t.TempDir()
	return &fakeStorageProvider{dirName}
}

type fakeStorageProvider struct {
	dataPath string
}

func (sp fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (sp fakeStorageProvider) DataPath() string {
	return sp.dataPath
}
