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

package client

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
	"github.com/weaviate/weaviate/modules/sum-transformers/ent"
)

type client struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

type sumInput struct {
	Text string `json:"text"`
}

type summaryResponse struct {
	// Property      string  `json:"property"`
	Result string `json:"result"`
}

type sumResponse struct {
	Error string
	sumInput
	Summary []summaryResponse `json:"summary"`
}

func New(origin string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		origin: origin,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (c *client) GetSummary(ctx context.Context, property, text string,
) ([]ent.SummaryResult, error) {
	body, err := json.Marshal(sumInput{
		Text: text,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.url("/sum/"),
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody sumResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode > 399 {
		return nil, errors.Errorf("fail with status %d: %s", res.StatusCode, resBody.Error)
	}

	out := make([]ent.SummaryResult, len(resBody.Summary))

	for i, elem := range resBody.Summary {
		out[i].Result = elem.Result
		out[i].Property = property
	}

	// format resBody to nerResult
	return out, nil
}

func (c *client) url(path string) string {
	return fmt.Sprintf("%s%s", c.origin, path)
}
