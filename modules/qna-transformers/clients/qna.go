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
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/modules/qna-transformers/ent"
	"github.com/sirupsen/logrus"
)

type qna struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, logger logrus.FieldLogger) *qna {
	return &qna{
		origin:     origin,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

func (v *qna) Answer(ctx context.Context,
	text, question string) (*ent.AnswerResult, error) {
	body, err := json.Marshal(answersInput{
		Text:     text,
		Question: question,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/answers/"),
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

	var resBody answersResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode > 399 {
		return nil, errors.Errorf("fail with status %d: %s", res.StatusCode,
			resBody.Error)
	}

	return &ent.AnswerResult{
		Text:      resBody.Text,
		Question:  resBody.Question,
		Answer:    resBody.Answer,
		Certainty: resBody.Certainty,
		Distance:  additional.CertaintyToDist(resBody.Certainty),
	}, nil
}

func (v *qna) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}

type answersInput struct {
	Text     string `json:"text"`
	Question string `json:"question"`
}

type answersResponse struct {
	answersInput `json:"answersInput"`
	Answer       *string  `json:"answer"`
	Certainty    *float64 `json:"certainty"`
	Distance     *float64 `json:"distance"`
	Error        string   `json:"error"`
}
