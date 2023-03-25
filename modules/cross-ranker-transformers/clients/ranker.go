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
	"github.com/weaviate/weaviate/modules/cross-ranker-transformers/ent"
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
	property string, query string,
) (*ent.RankResult, error) {
	body, err := json.Marshal(RankInput{
		Property: property,
		Query:    query,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/crossrank"),
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

	if res.StatusCode > 399 {
		return nil, errors.Errorf("fail with status %d: %s", res.StatusCode,
			resBody.Error)
	}

	return &ent.RankResult{
		Property: resBody.Property,
		Query:    resBody.Query,
		Score:    resBody.Score,
	}, nil
}

func (v *client) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}

type RankInput struct {
	Property string `json:"Property"`
	Query    string `json:"Query"`
}

type RankResponse struct {
	Property string  `json:"Property"`
	Query    string  `json:"Query"`
	Score    float64 `json:"Score"`
	Error    string  `json:"Error"`
}
