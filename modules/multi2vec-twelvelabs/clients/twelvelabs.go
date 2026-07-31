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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2vec-twelvelabs/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

type embedding struct {
	Segments []segment `json:"segments"`
}

type segment struct {
	Float []float32 `json:"float"`
}

// embedResponse mirrors the TwelveLabs Embed API response. Only one of the
// *_embedding fields is populated per request depending on the input type.
// See https://docs.twelvelabs.io/v1.3/api-reference/create-embeddings
type embedResponse struct {
	ModelName      string     `json:"model_name"`
	TextEmbedding  *embedding `json:"text_embedding"`
	ImageEmbedding *embedding `json:"image_embedding"`
	Code           string     `json:"code"`
	Message        string     `json:"message"`
}

type vectorizer struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey:     apiKey,
		httpClient: modulecomponents.NewBaseHttpClient(timeout),
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, texts, images, cfg)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context,
	input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, input, nil, cfg)
}

func (v *vectorizer) VectorizeImages(ctx context.Context,
	images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, nil, images, cfg)
}

// vectorize embeds texts and images with Marengo. The TwelveLabs Embed API
// accepts a single input per request, so we issue one request per item.
func (v *vectorizer) vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	settings := ent.NewClassSettings(cfg)
	baseURL := settings.BaseURL()
	model := settings.Model()

	var textVectors, imageVectors [][]float32
	for i := range texts {
		vec, err := v.embedText(ctx, baseURL, model, texts[i])
		if err != nil {
			return nil, err
		}
		textVectors = append(textVectors, vec)
	}
	for i := range images {
		vec, err := v.embedImage(ctx, baseURL, model, images[i])
		if err != nil {
			return nil, err
		}
		imageVectors = append(imageVectors, vec)
	}

	return &modulecomponents.VectorizationCLIPResult[[]float32]{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
	}, nil
}

func (v *vectorizer) embedText(ctx context.Context, baseURL, model, text string) ([]float32, error) {
	res, err := v.doEmbed(ctx, baseURL, model, func(w *multipart.Writer) error {
		return w.WriteField("text", text)
	})
	if err != nil {
		return nil, err
	}
	return extractEmbedding(res.TextEmbedding)
}

func (v *vectorizer) embedImage(ctx context.Context, baseURL, model, image string) ([]float32, error) {
	// Weaviate stores image properties as base64. The TwelveLabs Embed API
	// accepts the raw bytes via the image_file multipart field.
	img := image
	if idx := strings.Index(img, ","); strings.HasPrefix(img, "data:") && idx != -1 {
		img = img[idx+1:]
	}
	raw, err := base64.StdEncoding.DecodeString(img)
	if err != nil {
		return nil, errors.Wrap(err, "decode base64 image")
	}
	res, err := v.doEmbed(ctx, baseURL, model, func(w *multipart.Writer) error {
		part, err := w.CreateFormFile("image_file", "image")
		if err != nil {
			return err
		}
		_, err = part.Write(raw)
		return err
	})
	if err != nil {
		return nil, err
	}
	return extractEmbedding(res.ImageEmbedding)
}

func extractEmbedding(emb *embedding) ([]float32, error) {
	if emb == nil || len(emb.Segments) == 0 || len(emb.Segments[0].Float) == 0 {
		return nil, errors.New("empty embeddings response")
	}
	return emb.Segments[0].Float, nil
}

func (v *vectorizer) doEmbed(ctx context.Context, baseURL, model string,
	writeInput func(*multipart.Writer) error,
) (*embedResponse, error) {
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "TwelveLabs API Key")
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if err := writer.WriteField("model_name", model); err != nil {
		return nil, errors.Wrap(err, "write model_name field")
	}
	if err := writeInput(writer); err != nil {
		return nil, errors.Wrap(err, "write input field")
	}
	if err := writer.Close(); err != nil {
		return nil, errors.Wrap(err, "close multipart writer")
	}

	url := fmt.Sprintf("%s/embed", baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody embedResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrapf(err, "unmarshal response body. got: %v", string(bodyBytes))
	}

	if res.StatusCode != http.StatusOK {
		return nil, errors.New(v.getErrorMessage(res.StatusCode, resBody.Message))
	}
	return &resBody, nil
}

func (v *vectorizer) getErrorMessage(statusCode int, resBodyError string) string {
	if resBodyError != "" {
		return fmt.Sprintf("connection to TwelveLabs failed with status: %d error: %v", statusCode, resBodyError)
	}
	return fmt.Sprintf("connection to TwelveLabs failed with status: %d", statusCode)
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Twelvelabs-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-TwelveLabs-Api-Key " +
		"nor in environment variable under TWELVELABS_APIKEY")
}
