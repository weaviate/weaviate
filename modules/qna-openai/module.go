//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modqnaopenai

import (
	"context"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	qnaadditional "github.com/semi-technologies/weaviate/modules/qna-openai/additional"
	qnaadditionalanswer "github.com/semi-technologies/weaviate/modules/qna-openai/additional/answer"
	qnaask "github.com/semi-technologies/weaviate/modules/qna-openai/ask"
	"github.com/semi-technologies/weaviate/modules/qna-openai/clients"
	qnaadependency "github.com/semi-technologies/weaviate/modules/qna-openai/dependency"
	"github.com/semi-technologies/weaviate/modules/qna-openai/ent"
	"github.com/sirupsen/logrus"
)

const Name = "qna-openai"

func New() *QnAModule {
	return &QnAModule{}
}

type QnAModule struct {
	qna                          qnaClient
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.DependencySearcher
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
	nearTextDependencies         []modulecapabilities.Dependency
	askTextTransformer           modulecapabilities.TextTransform
}

type qnaClient interface {
	Answer(ctx context.Context, text, question string, cfg moduletools.ClassConfig) (*ent.AnswerResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *QnAModule) Name() string {
	return Name
}

func (m *QnAModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Text
}

func (m *QnAModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init q/a")
	}

	return nil
}

func (m *QnAModule) InitExtension(modules []modulecapabilities.Module) error {
	var textTransformer modulecapabilities.TextTransform
	for _, module := range modules {
		if module.Name() == m.Name() {
			continue
		}
		if arg, ok := module.(modulecapabilities.TextTransformers); ok {
			if arg != nil && arg.TextTransformers() != nil {
				textTransformer = arg.TextTransformers()["ask"]
			}
		}
	}

	m.askTextTransformer = textTransformer

	if err := m.initAskProvider(); err != nil {
		return errors.Wrap(err, "init ask provider")
	}

	return nil
}

func (m *QnAModule) InitDependency(modules []modulecapabilities.Module) error {
	nearTextDependencies := []modulecapabilities.Dependency{}
	for _, module := range modules {
		if module.Name() == m.Name() {
			continue
		}
		var argument modulecapabilities.GraphQLArgument
		var searcher modulecapabilities.VectorForParams
		if arg, ok := module.(modulecapabilities.GraphQLArguments); ok {
			if arg != nil && arg.Arguments() != nil {
				if nearTextArg, ok := arg.Arguments()["nearText"]; ok {
					argument = nearTextArg
				}
			}
		}
		if arg, ok := module.(modulecapabilities.Searcher); ok {
			if arg != nil && arg.VectorSearches() != nil {
				if nearTextSearcher, ok := arg.VectorSearches()["nearText"]; ok {
					searcher = nearTextSearcher
				}
			}
		}

		if argument.ExtractFunction != nil && searcher != nil {
			nearTextDependency := qnaadependency.New(module.Name(), argument, searcher)
			nearTextDependencies = append(nearTextDependencies, nearTextDependency)
		}
	}
	if len(nearTextDependencies) == 0 {
		return errors.New("nearText dependecy not present")
	}

	m.nearTextDependencies = nearTextDependencies

	if err := m.initAskSearcher(); err != nil {
		return errors.Wrap(err, "init ask searcher")
	}

	return nil
}

func (m *QnAModule) initAdditional(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("OPENAI_APIKEY")

	client := clients.New(apiKey, logger)

	m.qna = client

	answerProvider := qnaadditionalanswer.New(m.qna, qnaask.NewParamsHelper())
	m.additionalPropertiesProvider = qnaadditional.New(answerProvider)

	return nil
}

func (m *QnAModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *QnAModule) MetaInfo() (map[string]interface{}, error) {
	return m.qna.MetaInfo()
}

func (m *QnAModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
