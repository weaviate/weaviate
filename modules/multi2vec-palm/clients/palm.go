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

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/multi2vec-palm/ent"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

func buildURL(location, projectID, model string) string {
	return fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
		location, projectID, location, model)
}

type palm struct {
	apiKey       string
	httpClient   *http.Client
	urlBuilderFn func(location, projectID, model string) string
	logger       logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *palm {
	return &palm{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilderFn: buildURL,
		logger:       logger,
	}
}

func (v *palm) Vectorize(ctx context.Context,
	texts, images, videos []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, texts, images, videos, config)
}

func (v *palm) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, nil, nil, config)
}

func (v *palm) vectorize(ctx context.Context,
	texts, images, videos []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	var textEmbeddings [][]float32
	var imageEmbeddings [][]float32
	var videoEmbeddings [][]float32
	endpointURL := v.getURL(config)
	maxCount := max(len(texts), len(images), len(videos))
	for i := 0; i < maxCount; i++ {
		text := v.safelyGet(texts, i)
		image := v.safelyGet(images, i)
		video := v.safelyGet(videos, i)
		payload := v.getPayload(text, image, video, config)
		statusCode, res, err := v.sendRequest(ctx, endpointURL, payload)
		if err != nil {
			return nil, err
		}
		textVectors, imageVectors, videoVectors, err := v.getEmbeddingsFromResponse(statusCode, res)
		if err != nil {
			return nil, err
		}
		textEmbeddings = append(textEmbeddings, textVectors...)
		imageEmbeddings = append(imageEmbeddings, imageVectors...)
		videoEmbeddings = append(videoEmbeddings, videoVectors...)
	}

	return v.getResponse(textEmbeddings, imageEmbeddings, videoEmbeddings)
}

func (v *palm) safelyGet(input []string, i int) string {
	if i < len(input) {
		return input[i]
	}
	return ""
}

func (v *palm) sendRequest(ctx context.Context,
	endpointURL string, payload embeddingsRequest,
) (int, embeddingsResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, embeddingsResponse{}, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return 0, embeddingsResponse{}, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return 0, embeddingsResponse{}, errors.Wrapf(err, "Google API Key")
	}
	req.Header.Add("Content-Type", "application/json; charset=utf-8")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))

	res, err := v.httpClient.Do(req)
	if err != nil {
		return 0, embeddingsResponse{}, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, embeddingsResponse{}, errors.Wrap(err, "read response body")
	}

	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return 0, embeddingsResponse{}, errors.Wrap(err, "unmarshal response body")
	}

	return res.StatusCode, resBody, nil
}

func (v *palm) getURL(config ent.VectorizationConfig) string {
	return v.urlBuilderFn(config.Location, config.ProjectID, config.Model)
}

func (v *palm) getPayload(text, img, vid string, config ent.VectorizationConfig) embeddingsRequest {
	inst := instance{}
	if text != "" {
		inst.Text = &text
	}
	if img != "" {
		inst.Image = &image{BytesBase64Encoded: img}
	}
	if vid != "" {
		inst.Video = &video{
			BytesBase64Encoded: vid,
			VideoSegmentConfig: videoSegmentConfig{IntervalSec: &config.VideoIntervalSeconds},
		}
	}
	return embeddingsRequest{
		Instances:  []instance{inst},
		Parameters: parameters{Dimension: config.Dimensions},
	}
}

func (v *palm) checkResponse(statusCode int, palmApiError *palmApiError) error {
	if statusCode != 200 || palmApiError != nil {
		if palmApiError != nil {
			return fmt.Errorf("connection to Google failed with status: %v error: %v",
				statusCode, palmApiError.Message)
		}
		return fmt.Errorf("connection to Google failed with status: %d", statusCode)
	}
	return nil
}

func (v *palm) getApiKey(ctx context.Context) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, "X-Google-Api-Key"); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if apiKeyValue := v.getValueFromContext(ctx, "X-Palm-Api-Key"); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Palm-Api-Key or X-Google-Api-Key " +
		"nor in environment variable under PALM_APIKEY or GOOGLE_APIKEY")
}

func (v *palm) getValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	// try getting header from GRPC if not successful
	if apiKey := modulecomponents.GetValueFromGRPC(ctx, key); len(apiKey) > 0 && len(apiKey[0]) > 0 {
		return apiKey[0]
	}
	return ""
}

func (v *palm) getEmbeddingsFromResponse(statusCode int, resBody embeddingsResponse) (
	textEmbeddings [][]float32,
	imageEmbeddings [][]float32,
	videoEmbeddings [][]float32,
	err error,
) {
	if respErr := v.checkResponse(statusCode, resBody.Error); respErr != nil {
		err = respErr
		return
	}

	if len(resBody.Predictions) == 0 {
		err = errors.Errorf("empty embeddings response")
		return
	}

	for _, p := range resBody.Predictions {
		if len(p.TextEmbedding) > 0 {
			textEmbeddings = append(textEmbeddings, p.TextEmbedding)
		}
		if len(p.ImageEmbedding) > 0 {
			imageEmbeddings = append(imageEmbeddings, p.ImageEmbedding)
		}
		if len(p.VideoEmbeddings) > 0 {
			var embeddings [][]float32
			for _, videoEmbedding := range p.VideoEmbeddings {
				embeddings = append(embeddings, videoEmbedding.Embedding)
			}
			embedding := embeddings[0]
			if len(embeddings) > 1 {
				embedding = libvectorizer.CombineVectors(embeddings)
			}
			videoEmbeddings = append(videoEmbeddings, embedding)
		}
	}
	return
}

func (v *palm) getResponse(textVectors, imageVectors, videoVectors [][]float32) (*ent.VectorizationResult, error) {
	return &ent.VectorizationResult{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
		VideoVectors: videoVectors,
	}, nil
}

type embeddingsRequest struct {
	Instances  []instance `json:"instances,omitempty"`
	Parameters parameters `json:"parameters,omitempty"`
}

type parameters struct {
	Dimension int64 `json:"dimension,omitempty"`
}

type instance struct {
	Text  *string `json:"text,omitempty"`
	Image *image  `json:"image,omitempty"`
	Video *video  `json:"video,omitempty"`
}

type image struct {
	BytesBase64Encoded string `json:"bytesBase64Encoded"`
}

type video struct {
	BytesBase64Encoded string             `json:"bytesBase64Encoded"`
	VideoSegmentConfig videoSegmentConfig `json:"videoSegmentConfig"`
}

type videoSegmentConfig struct {
	StartOffsetSec *int64 `json:"startOffsetSec,omitempty"`
	EndOffsetSec   *int64 `json:"endOffsetSec,omitempty"`
	IntervalSec    *int64 `json:"intervalSec,omitempty"`
}

type embeddingsResponse struct {
	Predictions     []prediction  `json:"predictions,omitempty"`
	Error           *palmApiError `json:"error,omitempty"`
	DeployedModelId string        `json:"deployedModelId,omitempty"`
}

type prediction struct {
	TextEmbedding   []float32        `json:"textEmbedding,omitempty"`
	ImageEmbedding  []float32        `json:"imageEmbedding,omitempty"`
	VideoEmbeddings []videoEmbedding `json:"videoEmbeddings,omitempty"`
}

type videoEmbedding struct {
	StartOffsetSec *int64    `json:"startOffsetSec,omitempty"`
	EndOffsetSec   *int64    `json:"endOffsetSec,omitempty"`
	Embedding      []float32 `json:"embedding,omitempty"`
}

type palmApiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}
