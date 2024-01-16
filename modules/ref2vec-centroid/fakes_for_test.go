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

package modcentroid

import (
	"testing"

	"github.com/weaviate/weaviate/entities/moduletools"
)

type fakeClassConfig map[string]interface{}

func (cfg fakeClassConfig) Class() map[string]interface{} {
	return cfg
}

func (cfg fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return cfg
}

func (cfg fakeClassConfig) Property(string) map[string]interface{} {
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
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
