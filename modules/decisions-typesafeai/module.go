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

package moddecisionstypesafeai

import (
	"context"
	"maps"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/decisions-typesafeai/clients"
	rerankeradditional "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const Name = "decisions-typesafeai"

func New() *DecisionsTypeSafeAIModule {
	return &DecisionsTypeSafeAIModule{}
}

type DecisionsTypeSafeAIModule struct {
	reranker             DecisionsTypeSafeAIClient
	additionalProperties map[string]modulecapabilities.AdditionalProperty
}

type DecisionsTypeSafeAIClient interface {
	Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
	Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string,
		cfg moduletools.ClassConfig) ([][]ent.DecisionAnswer, error)
	MetaInfo() (map[string]any, error)
}

func (m *DecisionsTypeSafeAIModule) Name() string {
	return Name
}

func (m *DecisionsTypeSafeAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Decisions
}

func (m *DecisionsTypeSafeAIModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init decisions-typesafeai")
	}

	return nil
}

func (m *DecisionsTypeSafeAIModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("TYPESAFEAI_APIKEY")
	maxConcurrentRequests, err := maxConcurrentRequestsFromEnv()
	if err != nil {
		return err
	}
	m.setClient(clients.New(apiKey, timeout, maxConcurrentRequests, logger))
	return nil
}

const maxConcurrentRequestsEnv = "DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS"

// maxConcurrentRequestsFromEnv reads the process-wide limit of requests to
// the TypeSafeAI API that may be in flight at once.
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

func (m *DecisionsTypeSafeAIModule) setClient(client DecisionsTypeSafeAIClient) {
	m.reranker = client
	m.additionalProperties = withTypeSafeAIRerank(
		rerankeradditional.NewRankerProvider(client).AdditionalProperties(), rank.New(client))
	maps.Copy(m.additionalProperties, rerankeradditional.NewDecideProvider(client).AdditionalProperties())
}

func (m *DecisionsTypeSafeAIModule) MetaInfo() (map[string]any, error) {
	return m.reranker.MetaInfo()
}

func (m *DecisionsTypeSafeAIModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalProperties
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
