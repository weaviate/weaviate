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

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-transformers/ent"
)

type vectorizer struct {
	originPassage string
	originQuery   string
	httpClient    *http.Client
	logger        logrus.FieldLogger
}

func New(originPassage, originQuery string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		originPassage: originPassage,
		originQuery:   originQuery,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *vectorizer) VectorizeObject(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config, v.urlPassage)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config, v.urlQuery)
}

func (v *vectorizer) vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig, url func(string) string,
) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(vecRequest{
		Text: input,
		Config: vecRequestConfig{
			PoolingStrategy: config.PoolingStrategy,
		},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url("/vectors"),
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody vecRequest
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode > 399 {
		return nil, errors.Errorf("fail with status %d: %s", res.StatusCode,
			resBody.Error)
	}

	return &ent.VectorizationResult{
		Text:       resBody.Text,
		Dimensions: resBody.Dims,
		Vector:     resBody.Vector,
	}, nil
}

func (v *vectorizer) urlPassage(path string) string {
	return fmt.Sprintf("%s%s", v.originPassage, path)
}

func (v *vectorizer) urlQuery(path string) string {
	return fmt.Sprintf("%s%s", v.originQuery, path)
}

type vecRequest struct {
	Text   string           `json:"text"`
	Dims   int              `json:"dims"`
	Vector []float32        `json:"vector"`
	Error  string           `json:"error"`
	Config vecRequestConfig `json:"config"`
}

type vecRequestConfig struct {
	PoolingStrategy string `json:"pooling_strategy"`
}
