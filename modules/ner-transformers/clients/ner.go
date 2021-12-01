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
	"github.com/semi-technologies/weaviate/modules/ner-transformers/ent"
	"github.com/sirupsen/logrus"
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
	Word          string  `json:"word"`
	StartPosition int     `json:"startPosition"`
	EndPosition   int     `json:"endPosition"`
}

type nerResponse struct {
	Error string
	nerInput
	Tokens []tokenResponse `json:"tokens"`
}

func New(origin string, logger logrus.FieldLogger) *ner {
	return &ner{
		origin:     origin,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

func (v *ner) GetTokens(ctx context.Context, property,
	text string) ([]ent.TokenResult, error) {
	body, err := json.Marshal(nerInput{
		Text: text,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/ner/"),
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
		out[i].Entity = elem.Entity
		out[i].Word = elem.Word
		out[i].StartPosition = elem.StartPosition
		out[i].EndPosition = elem.EndPosition
		out[i].Property = property
	}

	// format resBody to nerResult
	return out, nil
}

func (v *ner) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}
