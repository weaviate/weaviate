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
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

func (s *ner) MetaInfo() (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(context.Background(), "GET", s.url("/meta"), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create GET meta request")
	}

	res, err := s.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send GET meta request")
	}
	defer res.Body.Close()

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read meta response body")
	}

	var resBody map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal meta response body")
	}
	return resBody, nil
}
