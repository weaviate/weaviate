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

	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/multi2vec-google/ent"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

func buildURL(location, projectID, model string) string {
	return fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
		location, projectID, location, model)
}

type google struct {
	apiKey        string
	useGoogleAuth bool
	googleApiKey  *apikey.GoogleApiKey
	httpClient    *http.Client
	urlBuilderFn  func(location, projectID, model string) string
	logger        logrus.FieldLogger
}

func New(apiKey string, useGoogleAuth bool, timeout time.Duration, logger logrus.FieldLogger) *google {
	return &google{
		apiKey:        apiKey,
		useGoogleAuth: useGoogleAuth,
		googleApiKey:  apikey.NewGoogleApiKey(),
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilderFn: buildURL,
		logger:       logger,
	}
}

func (v *google) Vectorize(ctx context.Context,
	texts, images, videos []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, texts, images, videos, config)
}

func (v *google) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, nil, nil, config)
}

func (v *google) vectorize(ctx context.Context,
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

func (v *google) safelyGet(input []string, i int) string {
	if i < len(input) {
		return input[i]
	}
	return ""
}

func (v *google) sendRequest(ctx context.Context,
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
		return 0, embeddingsResponse{}, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	return res.StatusCode, resBody, nil
}

func (v *google) getURL(config ent.VectorizationConfig) string {
	return v.urlBuilderFn(config.Location, config.ProjectID, config.Model)
}

func (v *google) getPayload(text, img, vid string, config ent.VectorizationConfig) embeddingsRequest {
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
	req := embeddingsRequest{
		Instances: []instance{inst},
	}
	if inst.Video == nil {
		req.Parameters = parameters{Dimension: config.Dimensions}
	}
	return req
}

func (v *google) checkResponse(statusCode int, googleApiError *googleApiError) error {
	if statusCode != 200 || googleApiError != nil {
		if googleApiError != nil {
			return fmt.Errorf("connection to Google failed with status: %v error: %v",
				statusCode, googleApiError.Message)
		}
		return fmt.Errorf("connection to Google failed with status: %d", statusCode)
	}
	return nil
}

func (v *google) getApiKey(ctx context.Context) (string, error) {
	return v.googleApiKey.GetApiKey(ctx, v.apiKey, false, v.useGoogleAuth)
}

func (v *google) getEmbeddingsFromResponse(statusCode int, resBody embeddingsResponse) (
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

func (v *google) getResponse(textVectors, imageVectors, videoVectors [][]float32) (*ent.VectorizationResult, error) {
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
	Predictions     []prediction    `json:"predictions,omitempty"`
	Error           *googleApiError `json:"error,omitempty"`
	DeployedModelId string          `json:"deployedModelId,omitempty"`
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

type googleApiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}
