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
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type metaInfoFn func() (map[string]interface{}, error)

func (v *vectorizer) MetaInfo() (map[string]interface{}, error) {
	metaInfoFns := v.allMetaInfoFns()

	switch len(metaInfoFns) {
	case 0:
		return nil, errors.Errorf("no metaInfo callback configured")
	case 1:
		for _, singleMetaInfo := range metaInfoFns {
			return singleMetaInfo()
		}
	}
	return v.metaInfoAll(metaInfoFns)
}

func (v *vectorizer) allMetaInfoFns() map[string]metaInfoFn {
	if v.originPassage != v.originQuery {
		return map[string]metaInfoFn{
			"passage": func() (map[string]interface{}, error) { return v.metaInfo(v.urlPassage) },
			"query":   func() (map[string]interface{}, error) { return v.metaInfo(v.urlQuery) },
		}
	}
	return map[string]metaInfoFn{
		"common": func() (map[string]interface{}, error) { return v.metaInfo(v.urlPassage) },
	}
}

func (v *vectorizer) metaInfoAll(metaInfoFns map[string]metaInfoFn) (map[string]interface{}, error) {
	type nameMetaErr struct {
		name string
		meta map[string]interface{}
		err  error
	}

	var wg sync.WaitGroup
	ch := make(chan nameMetaErr, len(metaInfoFns))
	for name, metaInfo := range metaInfoFns {
		wg.Add(1)
		go func(name string, metaInfo metaInfoFn) {
			defer wg.Done()
			meta, err := metaInfo()
			ch <- nameMetaErr{name, meta, err}
		}(name, metaInfo)
	}
	wg.Wait()
	close(ch)

	metas := map[string]interface{}{}
	var errs []string
	for nme := range ch {
		if nme.err != nil {
			errs = append(errs, fmt.Sprintf("[%s] %v", nme.name, nme.err.Error()))
		}
		if nme.meta != nil {
			metas[nme.name] = nme.meta
		}
	}

	if len(errs) > 0 {
		return nil, errors.Errorf(strings.Join(errs, ", "))
	}
	return metas, nil
}

func (v *vectorizer) metaInfo(url func(string) string) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(context.Background(), "GET", url("/meta"), nil)
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
