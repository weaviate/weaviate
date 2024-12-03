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
	"github.com/weaviate/weaviate/modules/multi2vec-voyageai/ent"
)

type vectorizer struct {
	apiKey     string
	httpClient *http.Client
	urlBuilder *voyageaiUrlBuilder
	logger     logrus.FieldLogger
}

type inputContent struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	ImageURL    string `json:"image_url,omitempty"`
	ImageBase64 string `json:"image_base64,omitempty"`
}

type input struct {
	Content []inputContent `json:"content"`
}

type embeddingsRequest struct {
	Inputs         []input `json:"inputs"`
	Model          string  `json:"model"`
	InputType      string  `json:"input_type,omitempty"`
	Truncation     bool    `json:"truncation,omitempty"`
	OutputEncoding string  `json:"output_encoding,omitempty"`
}

type embeddingData struct {
	Object    string    `json:"object"`
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type embeddingsResponse struct {
	Object string          `json:"object"`
	Data   []embeddingData `json:"data"`
	Model  string          `json:"model"`
	Usage  map[string]int  `json:"usage"`
	Detail string          `json:"detail,omitempty"`
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilder: newVoyageAIUrlBuilder(),
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, texts, images []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	inputs := make([]input, len(texts)+len(images))
	for i, text := range texts {
		content := inputContent{
			Type: "text",
			Text: text,
		}
		inputs[i] = input{
			Content: []inputContent{content},
		}
	}
	for i, image := range images {
		content := inputContent{
			Type:     "image_url",
			ImageURL: image,
		}
		inputs[len(texts)+i] = input{
			Content: []inputContent{content},
		}
	}

	body, err := json.Marshal(embeddingsRequest{
		Inputs: inputs,
		Model:  config.Model,
		//		InputType:      "text", // TODO: how to use document/query type in multimodal integration? Did not find in CLIP code
		Truncation:     config.Truncate,
		OutputEncoding: config.OutputEncoding,
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		v.urlBuilder.url(config.BaseURL), bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get API key")
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 {
		if resBody.Detail != "" {
			return nil, errors.Errorf("connection to VoyageAI failed with status: %d error: %v",
				res.StatusCode, resBody.Detail)
		}
		return nil, errors.Errorf("connection to VoyageAI failed with status: %d", res.StatusCode)
	}

	texts_embeddings := make([][]float32, len(texts))
	images_embeddings := make([][]float32, len(images))
	for i := range texts {
		texts_embeddings[i] = resBody.Data[i].Embedding
	}
	for i := range images {
		images_embeddings[i] = resBody.Data[len(texts)+i].Embedding
	}

	return &ent.VectorizationResult{
		TextVectors:  texts_embeddings,
		ImageVectors: images_embeddings,
	}, nil
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	if apiKey := v.getValueFromContext(ctx, "X-Voyageai-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-VoyageAI-Api-Key " +
		"nor in environment variable under VOYAGEAI_APIKEY")
}

func (v *vectorizer) getValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	return ""
}

func (v *vectorizer) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "VoyageAI Multi2Vec Module",
		"documentationHref": "https://docs.voyageai.com/docs/multimodal-embeddings",
	}, nil
}
