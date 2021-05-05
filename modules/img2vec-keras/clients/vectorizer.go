//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
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
	"github.com/semi-technologies/weaviate/modules/img2vec-keras/ent"
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

func (v *vectorizer) Vectorize(ctx context.Context,
	id, image string) (*ent.VectorizationResult, error) {
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

	bodyBytes, err := ioutil.ReadAll(res.Body)
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
