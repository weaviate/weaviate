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

package moddecisionsopenai

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/decisions-openai/clients"
	"github.com/weaviate/weaviate/modules/decisions-openai/config"
	rerankeradditional "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const Name = "decisions-openai"

func New() *DecisionsOpenAIModule {
	return &DecisionsOpenAIModule{}
}

type DecisionsOpenAIModule struct {
	reranker             DecisionsOpenAIClient
	additionalProperties map[string]modulecapabilities.AdditionalProperty
}

type DecisionsOpenAIClient interface {
	Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
	Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string,
		cfg moduletools.ClassConfig) ([][]ent.DecisionAnswer, error)
	MetaInfo() (map[string]any, error)
}

func (m *DecisionsOpenAIModule) Name() string {
	return Name
}

func (m *DecisionsOpenAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Decisions
}

func (m *DecisionsOpenAIModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init decisions-openai")
	}

	return nil
}

func (m *DecisionsOpenAIModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("OPENAI_APIKEY")
	organization := os.Getenv("OPENAI_ORGANIZATION")
	maxConcurrentRequests, err := maxConcurrentRequestsFromEnv()
	if err != nil {
		return err
	}
	m.reranker = clients.New(apiKey, organization, timeout, maxConcurrentRequests, logger)
	m.additionalProperties = rerankeradditional.NewDecisionsRankerProvider(m.reranker, decisionDefaults).AdditionalProperties()
	maps.Copy(m.additionalProperties, rerankeradditional.NewAnnotateProvider(m.reranker).AdditionalProperties())
	return nil
}

// decisionDefaults are the class settings a rerank uses where the request
// leaves a field unset.
func decisionDefaults(cfg moduletools.ClassConfig) (rank.DecisionSettings, error) {
	settings, err := config.NewClassSettings(cfg).Resolve()
	if err != nil {
		return rank.DecisionSettings{}, err
	}
	return rank.DecisionSettings{MinProbability: settings.MinProbability, Order: rank.Order(settings.Order)}, nil
}

const maxConcurrentRequestsEnv = "DECISIONS_OPENAI_MAX_CONCURRENT_REQUESTS"

// maxConcurrentRequestsFromEnv reads the process-wide limit of requests to
// the OpenAI API that may be in flight at once.
func maxConcurrentRequestsFromEnv() (int, error) {
	value := os.Getenv(maxConcurrentRequestsEnv)
	if value == "" {
		return clients.DefaultMaxConcurrentRequests, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 || parsed > clients.MaxConcurrentRequestsLimit {
		return 0, errors.Errorf("%s must be a whole number between 1 and %d, got %q",
			maxConcurrentRequestsEnv, clients.MaxConcurrentRequestsLimit, value)
	}
	return parsed, nil
}

func (m *DecisionsOpenAIModule) MetaInfo() (map[string]any, error) {
	return m.reranker.MetaInfo()
}

func (m *DecisionsOpenAIModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalProperties
}

// DecisionLimits: OpenAI publishes no caps for the Decisions API, so the
// Weaviate ceilings apply; the module judges text.
func (m *DecisionsOpenAIModule) DecisionLimits(cfg moduletools.ClassConfig) (modulecapabilities.DecisionLimits, error) {
	// A class created before the module was enabled was never validated,
	// so a setting Resolve rejects fails the request here, before the search.
	settings, err := config.NewClassSettings(cfg).Resolve()
	if err != nil {
		return modulecapabilities.DecisionLimits{}, err
	}
	if settings.MaxDocuments < 1 || settings.MaxDocuments > config.MaxDocumentsLimit {
		return modulecapabilities.DecisionLimits{}, fmt.Errorf("maxDocuments must be between 1 and %d, got %d", config.MaxDocumentsLimit, settings.MaxDocuments)
	}
	// The Decisions API judges text and inline images; a blob property
	// holds an image.
	return modulecapabilities.DecisionLimits{
		MaxDocuments: settings.MaxDocuments,
		DataTypes:    []schema.DataType{schema.DataTypeText, schema.DataTypeBlob},
	}, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.DecisionLimitsProvider(New())
)
