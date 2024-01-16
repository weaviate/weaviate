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
	"github.com/weaviate/weaviate/modules/multi2vec-bind/ent"
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
	texts, images, audio, video, imu, thermal, depth []string,
) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(vecRequest{
		Texts:   texts,
		Images:  images,
		Audio:   audio,
		Video:   video,
		IMU:     imu,
		Thermal: thermal,
		Depth:   depth,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/vectorize"),
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

	var resBody vecResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 {
		if resBody.Error != "" {
			return nil, errors.Errorf("fail with status %d: %s", res.StatusCode,
				resBody.Error)
		}
		return nil, errors.Errorf("fail with status %d", res.StatusCode)
	}

	return &ent.VectorizationResult{
		TextVectors:    resBody.TextVectors,
		ImageVectors:   resBody.ImageVectors,
		AudioVectors:   resBody.AudioVectors,
		VideoVectors:   resBody.VideoVectors,
		IMUVectors:     resBody.IMUVectors,
		ThermalVectors: resBody.ThermalVectors,
		DepthVectors:   resBody.DepthVectors,
	}, nil
}

func (v *vectorizer) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}

type vecRequest struct {
	Texts   []string `json:"texts,omitempty"`
	Images  []string `json:"images,omitempty"`
	Audio   []string `json:"audio,omitempty"`
	Video   []string `json:"video,omitempty"`
	IMU     []string `json:"imu,omitempty"`
	Thermal []string `json:"thermal,omitempty"`
	Depth   []string `json:"depth,omitempty"`
}

type vecResponse struct {
	TextVectors    [][]float32 `json:"textVectors,omitempty"`
	ImageVectors   [][]float32 `json:"imageVectors,omitempty"`
	AudioVectors   [][]float32 `json:"audioVectors,omitempty"`
	VideoVectors   [][]float32 `json:"videoVectors,omitempty"`
	IMUVectors     [][]float32 `json:"imuVectors,omitempty"`
	ThermalVectors [][]float32 `json:"thermalVectors,omitempty"`
	DepthVectors   [][]float32 `json:"depthVectors,omitempty"`
	Error          string      `json:"error,omitempty"`
}
