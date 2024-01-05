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
	"github.com/weaviate/weaviate/modules/ner-transformers/ent"
)

type ner struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

type nerInput struct {
	Text string `json:"text"`
}

type tokenResponse struct {
	// Property      string  `json:"property"`
	Entity        string  `json:"entity"`
	Certainty     float64 `json:"certainty"`
	Distance      float64 `json:"distance"`
	Word          string  `json:"word"`
	StartPosition int     `json:"startPosition"`
	EndPosition   int     `json:"endPosition"`
}

type nerResponse struct {
	Error string
	nerInput
	Tokens []tokenResponse `json:"tokens"`
}

func New(origin string, timeout time.Duration, logger logrus.FieldLogger) *ner {
	return &ner{
		origin:     origin,
		httpClient: &http.Client{Timeout: timeout},
		logger:     logger,
	}
}

func (n *ner) GetTokens(ctx context.Context, property,
	text string,
) ([]ent.TokenResult, error) {
	body, err := json.Marshal(nerInput{
		Text: text,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", n.url("/ner/"),
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := n.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody nerResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode > 399 {
		return nil, errors.Errorf("fail with status %d: %s", res.StatusCode, resBody.Error)
	}

	out := make([]ent.TokenResult, len(resBody.Tokens))

	for i, elem := range resBody.Tokens {
		out[i].Certainty = elem.Certainty
		out[i].Distance = elem.Distance
		out[i].Entity = elem.Entity
		out[i].Word = elem.Word
		out[i].StartPosition = elem.StartPosition
		out[i].EndPosition = elem.EndPosition
		out[i].Property = property
	}

	// format resBody to nerResult
	return out, nil
}

func (n *ner) url(path string) string {
	return fmt.Sprintf("%s%s", n.origin, path)
}
