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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/modules/qna-transformers/ent"
)

type qna struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, timeout time.Duration, logger logrus.FieldLogger) *qna {
	return &qna{
		origin:     origin,
		httpClient: &http.Client{Timeout: timeout},
		logger:     logger,
	}
}

func (q *qna) Answer(ctx context.Context,
	text, question string,
) (*ent.AnswerResult, error) {
	body, err := json.Marshal(answersInput{
		Text:     text,
		Question: question,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", q.url("/answers/"),
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := q.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
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
		Distance:  additional.CertaintyToDistPtr(resBody.Certainty),
	}, nil
}

func (q *qna) url(path string) string {
	return fmt.Sprintf("%s%s", q.origin, path)
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
