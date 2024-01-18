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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

func (v *vectorizer) MetaInfo() (map[string]interface{}, error) {
	type nameMetaErr struct {
		name string
		meta map[string]interface{}
		err  error
	}

	endpoints := map[string]string{}
	if v.originPassage != v.originQuery {
		endpoints["passage"] = v.urlPassage("/meta")
		endpoints["query"] = v.urlQuery("/meta")
	} else {
		endpoints[""] = v.urlPassage("/meta")
	}

	var wg sync.WaitGroup
	ch := make(chan nameMetaErr, len(endpoints))
	for serviceName, endpoint := range endpoints {
		wg.Add(1)
		go func(serviceName string, endpoint string) {
			defer wg.Done()
			meta, err := v.metaInfo(endpoint)
			ch <- nameMetaErr{serviceName, meta, err}
		}(serviceName, endpoint)
	}
	wg.Wait()
	close(ch)

	metas := map[string]interface{}{}
	var errs []string
	for nme := range ch {
		if nme.err != nil {
			prefix := ""
			if nme.name != "" {
				prefix = "[" + nme.name + "] "
			}
			errs = append(errs, fmt.Sprintf("%s%v", prefix, nme.err.Error()))
		}
		if nme.meta != nil {
			metas[nme.name] = nme.meta
		}
	}

	if len(errs) > 0 {
		return nil, errors.Errorf(strings.Join(errs, ", "))
	}
	if len(metas) == 1 {
		for _, meta := range metas {
			return meta.(map[string]interface{}), nil
		}
	}
	return metas, nil
}

func (v *vectorizer) metaInfo(endpoint string) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(context.Background(), "GET", endpoint, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create GET meta request")
	}

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send GET meta request")
	}
	defer res.Body.Close()
	if !(res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices) {
		return nil, errors.Errorf("unexpected status code '%d' of meta request", res.StatusCode)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read meta response body")
	}

	var resBody map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal meta response body")
	}
	return resBody, nil
}
