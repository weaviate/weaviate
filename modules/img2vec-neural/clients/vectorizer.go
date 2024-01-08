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
	"github.com/weaviate/weaviate/modules/img2vec-neural/ent"
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
	id, image string,
) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(vecRequest{
		ID:    id,
		Image: image,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/vectors"),
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

	if res.StatusCode > 399 {
		return nil, errors.Errorf("fail with status %d", res.StatusCode)
	}

	return &ent.VectorizationResult{
		ID:         resBody.ID,
		Image:      image,
		Dimensions: resBody.Dim,
		Vector:     resBody.Vector,
	}, nil
}

func (v *vectorizer) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}

type vecRequest struct {
	ID    string `json:"id"`
	Image string `json:"image"`
}

type vecResponse struct {
	ID     string    `json:"id"`
	Vector []float32 `json:"vector"`
	Dim    int       `json:"dim"`
	Error  string    `json:"error"`
}
