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
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	enitiesSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

// plainReranker is a reranker module that records in served when it runs.
type plainReranker struct {
	dummyNonVectorizerModule
	served *[]string
}

func (m plainReranker) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m plainReranker) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return rerankProperty(m.name, m.served)
}

// rerankProperty is a rerank additional property that records in served
// which module ran it.
func rerankProperty(module string, served *[]string) map[string]modulecapabilities.AdditionalProperty {
	fn := func(ctx context.Context, in []search.Result, params any, limit *int,
		argumentModuleParams map[string]any, cfg moduletools.ClassConfig,
	) ([]search.Result, error) {
		if served != nil {
			*served = append(*served, module)
		}
		return in, nil
	}
	return map[string]modulecapabilities.AdditionalProperty{
		"rerank": {SearchFunctions: modulecapabilities.AdditionalSearch{ExploreGet: fn, ExploreList: fn}},
	}
}

func rerankSchemaReader(t *testing.T) *local.MockSchemaReader {
	sch := enitiesSchema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:        "ClassA",
					ModuleConfig: map[string]interface{}{"reranker-a": map[string]interface{}{"fetchDepth": 30}},
				},
				{
					Class: "ClassBoth",
					ModuleConfig: map[string]interface{}{
						"reranker-a": map[string]interface{}{"fetchDepth": 30},
						"reranker-z": map[string]interface{}{"fetchDepth": 55},
					},
				},
				{
					Class:        "ClassNone",
					ModuleConfig: map[string]interface{}{},
				},
			},
		},
	}
	return newMockSchemaReader(t, sch)
}

// A class that names two rerankers must get the same one on every request.
func TestRerankModuleChoiceIsStable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var served []string
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(rerankSchemaReader(t))
	for _, name := range []string{"reranker-m", "reranker-z", "reranker-a"} {
		p.Register(plainReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: name}, served: &served})
	}
	in := []search.Result{{ClassName: "ClassBoth"}}

	for range additionalExtendRuns {
		_, err := p.GetExploreAdditionalExtend(context.Background(), in, map[string]any{"rerank": "params"}, nil, nil)
		require.NoError(t, err)
	}

	require.Len(t, served, additionalExtendRuns)
	for _, module := range served {
		require.Equal(t, "reranker-z", module)
	}
}
