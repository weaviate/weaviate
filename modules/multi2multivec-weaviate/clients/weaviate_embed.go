//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/sirupsen/logrus"
)

const (
	defaultRPM = 500 // from https://jina.ai/embeddings/
	defaultTPM = 1_000_000
)

type vectorizer struct {
	// client *jinaai.Client[[][]float32]
	logger logrus.FieldLogger
}

func New(jinaAIApiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		// client: jinaai.New[[][]float32](jinaAIApiKey, timeout, defaultRPM, defaultTPM, jinaai.EmbeddingsBuildUrlFn, logger),
		logger: logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[][]float32], error) {

	v.logger.Debug("***************** COLQWEN *****************")
	// settings := ent.NewClassSettings(cfg)
	// res, err := v.client.VectorizeMultiModal(ctx, texts, images, jinaai.Settings{
	// 	BaseURL:           settings.BaseURL(),
	// 	Model:             settings.Model(),
	// 	Task:              jinaai.RetrievalPassage,
	// 	Normalized:        true,
	// 	ReturnMultivector: true,
	// })
	// return res, err

	vectors, err := v.vectorize(ctx, images, false)
	return &modulecomponents.VectorizationCLIPResult[[][]float32]{
		TextVectors:  nil,
		ImageVectors: vectors,
	}, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[][]float32], error) {
	// settings := ent.NewClassSettings(cfg)
	// res, _, _, err := v.client.Vectorize(ctx, texts, jinaai.Settings{
	// 	BaseURL:           settings.BaseURL(),
	// 	Model:             settings.Model(),
	// 	Task:              jinaai.RetrievalQuery,
	// 	Normalized:        true,
	// 	ReturnMultivector: true,
	// })
	// return res, err

	vectors, err := v.vectorize(ctx, input, true)
	return &modulecomponents.VectorizationResult[[][]float32]{
		Text:   input,
		Vector: vectors,
	}, err
}

// TODO: pass module configs
func (v *vectorizer) vectorize(ctx context.Context, input []string, query bool) ([][][]float32, error) {
	body, err := json.Marshal(map[string]interface{}{
		"input": input,
		"query": query,
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "http://localhost:8000/embed",
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	client := &http.Client{Timeout: 600 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	if res.StatusCode != 200 {
		return nil, errors.Errorf("API request failed with status %d: %s", res.StatusCode, string(bodyBytes))
	}

	var vectors [][][]float32
	if err := json.Unmarshal(bodyBytes, &vectors); err != nil {
		return nil, errors.Wrap(err, "unmarshal response")
	}

	return vectors, nil
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	// TODO
	return [32]byte{}
	// return v.client.GetApiKeyHash(ctx, config)
}

// TODO: reuse
func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	// return v.client.GetVectorizerRateLimit(ctx, cfg)

	rpm, tpm := modulecomponents.GetRateLimitFromContext(ctx, "Weaviate", defaultRPM, defaultTPM)

	execAfterRequestFunction := func(limits *modulecomponents.RateLimits, tokensUsed int, deductRequest bool) {
		// refresh is after 60 seconds but leave a bit of room for errors. Otherwise, we only deduct the request that just happened
		if limits.LastOverwrite.Add(61 * time.Second).After(time.Now()) {
			if deductRequest {
				limits.RemainingRequests--
			}
			return
		}

		limits.RemainingRequests = rpm
		limits.ResetRequests = time.Now().Add(time.Duration(61) * time.Second)
		limits.LimitRequests = rpm
		limits.LastOverwrite = time.Now()

		limits.RemainingTokens = tpm
		limits.LimitTokens = tpm
		limits.ResetTokens = time.Now().Add(time.Duration(1) * time.Second)
	}

	initialRL := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	initialRL.ResetAfterRequestFunction(0) // set initial values

	return initialRL
}
