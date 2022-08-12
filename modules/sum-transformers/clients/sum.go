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
	"github.com/semi-technologies/weaviate/modules/sum-transformers/ent"
	"github.com/sirupsen/logrus"
)

type sum struct {
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

func New(origin string, logger logrus.FieldLogger) *sum {
	return &sum{
		origin:     origin,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

func (v *sum) GetSummary(ctx context.Context, property,
	text string) ([]ent.SummaryResult, error) {
	body, err := json.Marshal(sumInput{
		Text: text,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/sum/"),
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

func (v *sum) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}
