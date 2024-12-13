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
	"github.com/weaviate/weaviate/modules/multi2vec-clip/ent"
)

type vectorizer struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		origin: origin,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context,
	texts, images []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(vecRequest{
		Texts:  texts,
		Images: images,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/vectorize", config.InferenceURL),
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	req.Header.Set("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody vecResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode > 399 {
		return nil, errors.Errorf("fail with status %d: %s", res.StatusCode,
			resBody.Error)
	}

	return &ent.VectorizationResult{
		TextVectors:  resBody.TextVectors,
		ImageVectors: resBody.ImageVectors,
	}, nil
}

func (v *vectorizer) url(path string, inferenceURL string) string {
	if inferenceURL != "" {
		return fmt.Sprintf("%s%s", inferenceURL, path)
	}
	return fmt.Sprintf("%s%s", v.origin, path)
}

type vecRequest struct {
	Texts  []string `json:"texts,omitempty"`
	Images []string `json:"images,omitempty"`
}

type vecResponse struct {
	TextVectors  [][]float32 `json:"textVectors"`
	ImageVectors [][]float32 `json:"imageVectors"`
	Error        string      `json:"error"`
}
