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
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/config"
)

// droppingReranker is a reranker module that reads its fetch depth from the
// class config it is given.
type droppingReranker struct {
	dummyNonVectorizerModule
	err error
}

func (m droppingReranker) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m droppingReranker) RerankFetchDepth(ctx context.Context, cfg moduletools.ClassConfig, pageEnd int) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	// 7 stands for the module's default when the class sets nothing.
	depth, ok := cfg.Class()["fetchDepth"].(int)
	if !ok {
		return 7, nil
	}
	return depth, nil
}

func (m droppingReranker) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return rerankProperty(m.name, nil)
}

func TestRerankFetchDepth(t *testing.T) {
	tests := []struct {
		name      string
		modules   []modulecapabilities.Module
		className string
		want      int
		wantErr   string
	}{
		{
			name:      "the reranker named in the class config answers",
			modules:   []modulecapabilities.Module{droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}}},
			className: "ClassA",
			want:      30,
		},
		{
			name: "with several rerankers only the one named in the class config answers",
			modules: []modulecapabilities.Module{
				droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-b"}},
				droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}},
			},
			className: "ClassA",
			want:      30,
		},
		{
			name:      "the only enabled reranker answers for a class that names none",
			modules:   []modulecapabilities.Module{droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}}},
			className: "ClassNone",
			want:      7,
		},
		{
			name: "several rerankers and a class that names none gives 0",
			modules: []modulecapabilities.Module{
				droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}},
				droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-b"}},
			},
			className: "ClassNone",
			want:      0,
		},
		{
			// reranker-z serves the rerank for ClassBoth, so the depth that
			// reranker-a would ask for must not be used.
			name: "a class naming two rerankers uses the depth of the one that serves the rerank",
			modules: []modulecapabilities.Module{
				droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}},
				plainReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-z"}},
			},
			className: "ClassBoth",
			want:      0,
		},
		{
			name: "a class naming two rerankers: the serving one drops results",
			modules: []modulecapabilities.Module{
				plainReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}},
				droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-z"}},
			},
			className: "ClassBoth",
			want:      55,
		},
		{
			name:      "a reranker without the capability gives 0",
			modules:   []modulecapabilities.Module{plainReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}}},
			className: "ClassA",
			want:      0,
		},
		{
			name:      "no reranker gives 0",
			modules:   nil,
			className: "ClassA",
			want:      0,
		},
		{
			name: "the module error is returned with the module name",
			modules: []modulecapabilities.Module{droppingReranker{
				dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"},
				err:                      errors.New("bad header"),
			}},
			className: "ClassA",
			wantErr:   "module 'reranker-a': bad header",
		},
		{
			name:      "unknown class",
			modules:   []modulecapabilities.Module{droppingReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}}},
			className: "Missing",
			wantErr:   `class "Missing" not found in schema`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			p.SetSchemaGetter(rerankSchemaReader(t))
			for _, module := range tt.modules {
				p.Register(module)
			}

			got, err := p.RerankFetchDepth(context.Background(), tt.className, 10)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
