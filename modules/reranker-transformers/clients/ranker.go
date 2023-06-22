//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/reranker-transformers/ent"
)

type client struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, logger logrus.FieldLogger) *client {
	return &client{
		origin:     origin,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

func (v *client) Rank(ctx context.Context,
	rankpropertyValue string, query string,
) (*ent.RankResult, error) {
	body, err := json.Marshal(RankInput{
		RankPropertyValue: rankpropertyValue,
		Query:             query,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/rerank"),
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

	var resBody RankResponse
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

	return &ent.RankResult{
		RankPropertyValue: resBody.RankPropertyValue,
		Query:             resBody.Query,
		Score:             resBody.Score,
	}, nil
}

func (v *client) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}

type RankInput struct {
	RankPropertyValue string `json:"property"`
	Query             string `json:"query"`
}

type RankResponse struct {
	RankPropertyValue string  `json:"property"`
	Query             string  `json:"query"`
	Score             float64 `json:"score"`
	Error             string  `json:"error"`
}
