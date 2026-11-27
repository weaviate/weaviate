//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modules

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
)

type metaBackupModule struct{ dummyBackupModuleWithAltNames }

func (metaBackupModule) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{"bucketName": "internal-bucket"}, nil
}

type metaVectorizerModule struct{ dummyNonVectorizerModule }

func (metaVectorizerModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (metaVectorizerModule) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{"name": "vectorizer"}, nil
}

func TestGetMetaRedactInternals(t *testing.T) {
	cases := []struct {
		name       string
		redact     bool
		wantBackup map[string]interface{}
	}{
		{name: "off", wantBackup: map[string]interface{}{"bucketName": "internal-bucket"}},
		{name: "on", redact: true, wantBackup: map[string]interface{}{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{RedactInternals: tc.redact})
			p.Register(&metaBackupModule{})
			p.Register(&metaVectorizerModule{newDummyNonVectorizerModule("vec")})

			meta, err := p.GetMeta()
			require.NoError(t, err)
			require.Equal(t, map[string]interface{}{
				"SomeBackend": tc.wantBackup,
				"vec":         map[string]interface{}{"name": "vectorizer"},
			}, meta)
		})
	}
}
