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
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

func buildURL(apiEndpoint, location, projectID, model string) string {
	if apiEndpoint == "generativelanguage.googleapis.com" {
		return fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:batchEmbedContents", model)
	}
	return fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
		location, projectID, location, model)
}

type google struct {
	apiKey        string
	useGoogleAuth bool
	googleApiKey  *apikey.GoogleApiKey
	httpClient    *http.Client
	urlBuilderFn  func(apiEndpoint, location, projectID, model string) string
	logger        logrus.FieldLogger
}

func New(apiKey string, useGoogleAuth bool, timeout time.Duration, logger logrus.FieldLogger) *google {
	return &google{
		apiKey:        apiKey,
		useGoogleAuth: useGoogleAuth,
		googleApiKey:  apikey.NewGoogleApiKey(),
		httpClient:    modulecomponents.NewBaseHttpClient(timeout),
		urlBuilderFn:  buildURL,
		logger:        logger,
	}
}

func (v *google) Vectorize(ctx context.Context,
	texts, images, videos, audios []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, texts, images, videos, audios, config)
}

func (v *google) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, nil, nil, nil, config)
}

func (v *google) vectorize(ctx context.Context,
	texts, images, videos, audios []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	var textEmbeddings [][]float32
	var imageEmbeddings [][]float32
	var videoEmbeddings [][]float32
	var audioEmbeddings [][]float32
	endpointURL := v.getURL(config)
	maxCount := max(len(texts), len(images), len(videos), len(audios))
	for i := 0; i < maxCount; i++ {
		text := v.safelyGet(texts, i)
		image := v.safelyGet(images, i)
		video := v.safelyGet(videos, i)
		audio := v.safelyGet(audios, i)
		payload := v.getPayload(text, image, video, audio, config)
		statusCode, res, err := v.sendRequest(ctx, endpointURL, payload, config)
		if err != nil {
			return nil, err
		}

		var textVectors, imageVectors, videoVectors, audioVectors [][]float32
		if v.useGeminiApi(config) {
			textVectors, imageVectors, videoVectors, audioVectors, err = v.getEmbeddingsFromGeminiResponse(statusCode, res, text, image, video, audio)
			if err != nil {
				return nil, err
			}
		} else {
			// Vertex AI doesn't support audio files
			textVectors, imageVectors, videoVectors, err = v.getEmbeddingsFromVertexResponse(statusCode, res)
			if err != nil {
				return nil, err
			}
		}

		textEmbeddings = append(textEmbeddings, textVectors...)
		imageEmbeddings = append(imageEmbeddings, imageVectors...)
		videoEmbeddings = append(videoEmbeddings, videoVectors...)
		audioEmbeddings = append(audioEmbeddings, audioVectors...)
	}

	return v.getResponse(textEmbeddings, imageEmbeddings, videoEmbeddings, audioEmbeddings)
}

func (v *google) safelyGet(input []string, i int) string {
	if i < len(input) {
		return input[i]
	}
	return ""
}

func (v *google) sendRequest(ctx context.Context,
	endpointURL string, payload any,
	config ent.VectorizationConfig,
) (int, []byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return 0, nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "Google API Key")
	}
	req.Header.Add("Content-Type", "application/json; charset=utf-8")
	if v.useGeminiApi(config) {
		req.Header.Add("x-goog-api-key", apiKey)
	} else {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	}

	res, err := v.httpClient.Do(req)
	if err != nil {
		return 0, nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, nil, errors.Wrap(err, "read response body")
	}

	return res.StatusCode, bodyBytes, nil
}

func (v *google) getURL(config ent.VectorizationConfig) string {
	return v.urlBuilderFn(config.ApiEndpoint, config.Location, config.ProjectID, config.Model)
}

func (v *google) useGeminiApi(config ent.VectorizationConfig) bool {
	return config.ApiEndpoint == "generativelanguage.googleapis.com"
}

func (v *google) getPayload(text, img, vid, audio string, config ent.VectorizationConfig) any {
	if v.useGeminiApi(config) {
		return v.getGeminiPayload(text, img, vid, audio, config)
	}
	return v.getVertexPayload(text, img, vid, audio, config)
}

func (v *google) getVertexPayload(text, img, vid, audio string, config ent.VectorizationConfig) embeddingsRequest {
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
			VideoSegmentConfig: videoSegmentConfig{IntervalSec: config.VideoIntervalSeconds},
		}
	}
	req := embeddingsRequest{
		Instances: []instance{inst},
	}
	if inst.Video == nil {
		req.Parameters = &parameters{Dimension: config.Dimensions}
	}
	return req
}

func (v *google) getGeminiPayload(text, img, vid, audio string, config ent.VectorizationConfig) *batchEmbedContents {
	var parts []contentPart
	if text != "" {
		parts = append(parts, contentPart{Text: &text})
	}
	if img != "" {
		parts = append(parts, contentPart{InlineData: &inlineData{MimeType: "image/jpeg", Data: img}})
	}
	if vid != "" {
		parts = append(parts, contentPart{InlineData: &inlineData{MimeType: "video/mp4", Data: vid}})
	}
	if audio != "" {
		parts = append(parts, contentPart{InlineData: &inlineData{MimeType: "audio/mpeg", Data: audio}})
	}

	var requests []embedContentRequest
	for _, p := range parts {
		requests = append(requests, embedContentRequest{
			Model:                fmt.Sprintf("models/%s", config.Model),
			Content:              embedContent{Parts: []contentPart{p}},
			OutputDimensionality: config.Dimensions,
		})
	}
	return &batchEmbedContents{
		Requests: requests,
	}
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

func (v *google) getEmbeddingsFromVertexResponse(statusCode int, bodyBytes []byte) (
	textEmbeddings [][]float32,
	imageEmbeddings [][]float32,
	videoEmbeddings [][]float32,
	err error,
) {
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, nil, nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if respErr := v.checkResponse(statusCode, resBody.Error); respErr != nil {
		err = respErr
		return textEmbeddings, imageEmbeddings, videoEmbeddings, err
	}

	if len(resBody.Predictions) == 0 {
		err = errors.Errorf("empty embeddings response")
		return textEmbeddings, imageEmbeddings, videoEmbeddings, err
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
	return textEmbeddings, imageEmbeddings, videoEmbeddings, err
}

func (v *google) getEmbeddingsFromGeminiResponse(statusCode int, bodyBytes []byte,
	text, image, video, audio string,
) (
	textEmbeddings [][]float32,
	imageEmbeddings [][]float32,
	videoEmbeddings [][]float32,
	audioEmbeddings [][]float32,
	err error,
) {
	var resBody batchEmbedResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if respErr := v.checkResponse(statusCode, resBody.Error); respErr != nil {
		err = respErr
		return nil, nil, nil, nil, err
	}

	if len(resBody.Embeddings) == 0 {
		err = errors.Errorf("empty embeddings response")
		return nil, nil, nil, nil, err
	}

	for _, p := range resBody.Embeddings {
		if text != "" && len(textEmbeddings) == 0 {
			textEmbeddings = append(textEmbeddings, p.Values)
		} else if image != "" && len(imageEmbeddings) == 0 {
			imageEmbeddings = append(imageEmbeddings, p.Values)
		} else if video != "" && len(videoEmbeddings) == 0 {
			videoEmbeddings = append(videoEmbeddings, p.Values)
		} else if audio != "" && len(audioEmbeddings) == 0 {
			audioEmbeddings = append(audioEmbeddings, p.Values)
		}
	}
	return textEmbeddings, imageEmbeddings, videoEmbeddings, audioEmbeddings, err
}

func (v *google) getResponse(textVectors, imageVectors, videoVectors, audioVectors [][]float32) (*ent.VectorizationResult, error) {
	return &ent.VectorizationResult{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
		VideoVectors: videoVectors,
		AudioVectors: audioVectors,
	}, nil
}

type embeddingsRequest struct {
	Instances  []instance  `json:"instances,omitempty"`
	Parameters *parameters `json:"parameters,omitempty"`
}

type parameters struct {
	Dimension *int64 `json:"dimension,omitempty"`
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

// Gemini request
type batchEmbedContents struct {
	Requests []embedContentRequest `json:"requests,omitempty"`
}

type embedContentRequest struct {
	Model                string       `json:"model"`
	Content              embedContent `json:"content"`
	TaskType             *string      `json:"taskType,omitempty"`
	Title                string       `json:"title,omitempty"`
	OutputDimensionality *int64       `json:"outputDimensionality,omitempty"`
}

type embedContent struct {
	Parts []contentPart `json:"parts"`
}

type contentPart struct {
	InlineData *inlineData `json:"inline_data,omitempty"`
	Text       *string     `json:"text,omitempty"`
}

type inlineData struct {
	MimeType string `json:"mime_type"`
	Data     string `json:"data"`
}

// Gemini response
type batchEmbedResponse struct {
	Embeddings []embedContentEmbedding `json:"embeddings,omitempty"`
	Error      *googleApiError         `json:"error,omitempty"`
}

type embedContentEmbedding struct {
	Values []float32 `json:"values"`
}
