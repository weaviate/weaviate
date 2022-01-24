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
	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/ent"
	"github.com/sirupsen/logrus"
)

type vectorizer struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		origin:     origin,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(vecRequest{
		Text: input,
		Config: vecRequestConfig{
			PoolingStrategy: config.PoolingStrategy,
		},
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

	bodyBytes, err := ioutil.ReadAll(res.Body)
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

func (v *vectorizer) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
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
