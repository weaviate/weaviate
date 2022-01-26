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

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/modules/text2vec-openai/ent"
	"github.com/sirupsen/logrus"
)

type vectorizer struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey:     apiKey,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, v.docUrl(config.Type, config.Model))
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input string,
	config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, v.queryUrl(config.Type, config.Model))
}

func (v *vectorizer) vectorize(ctx context.Context, input string,
	url string) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(embeddingsRequest{
		Input: input,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", v.apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody embedding
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode > 399 {
		return nil, errors.Errorf("failed with status %d", res.StatusCode)
	}

	if len(resBody.Data) != 1 {
		return nil, errors.Errorf("wrong number of embeddings: %v", len(resBody.Data))
	}

	return &ent.VectorizationResult{
		Text:       input,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     resBody.Data[0].Embedding,
	}, nil
}

func (v *vectorizer) docUrl(docType, model string) string {
	return newDocumentVectorizerUrl(docType, model).url()
}

func (v *vectorizer) queryUrl(docType, model string) string {
	return newQueryVectorizerUrl(docType, model).url()
}

type embeddingsRequest struct {
	Input string `json:"input"`
}

type embedding struct {
	Object string          `json:"object"`
	Data   []embeddingData `json:"data"`
}

type embeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}
