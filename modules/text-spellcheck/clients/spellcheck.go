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
	"github.com/weaviate/weaviate/modules/text-spellcheck/ent"
)

type spellCheckInput struct {
	Text []string `json:"text"`
}

type spellCheckCorrection struct {
	Original   string `json:"original"`
	Correction string `json:"correction"`
}

type spellCheckResponse struct {
	spellCheckInput
	Changes []spellCheckCorrection `json:"changes"`
}

type spellCheck struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, timeout time.Duration, logger logrus.FieldLogger) *spellCheck {
	return &spellCheck{
		origin: origin,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (s *spellCheck) Check(ctx context.Context, text []string) (*ent.SpellCheckResult, error) {
	body, err := json.Marshal(spellCheckInput{
		Text: text,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.url("/spellcheck/"),
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := s.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody spellCheckResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode > 399 {
		return nil, errors.Errorf("fail with status %d", res.StatusCode)
	}

	return &ent.SpellCheckResult{
		Text:    resBody.Text,
		Changes: s.getCorrections(resBody.Changes),
	}, nil
}

func (s *spellCheck) url(path string) string {
	return fmt.Sprintf("%s%s", s.origin, path)
}

func (s *spellCheck) getCorrections(changes []spellCheckCorrection) []ent.SpellCheckCorrection {
	if len(changes) == 0 {
		return nil
	}
	corrections := make([]ent.SpellCheckCorrection, len(changes))
	for i := range changes {
		corrections[i] = ent.SpellCheckCorrection{
			Original:   changes[i].Original,
			Correction: changes[i].Correction,
		}
	}
	return corrections
}
