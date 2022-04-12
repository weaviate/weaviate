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
	type nameMeta struct {
		name string
		meta map[string]interface{}
	}
	type nameErr struct {
		name string
		err  error
	}

	var wg sync.WaitGroup
	chMeta := make(chan nameMeta, len(metaInfoFns))
	chErr := make(chan nameErr, len(metaInfoFns))

	for name, metaInfo := range metaInfoFns {
		wg.Add(1)
		go func(name string, metaInfo metaInfoFn) {
			defer wg.Done()
			if meta, err := metaInfo(); err == nil {
				chMeta <- nameMeta{name, meta}
			} else {
				chErr <- nameErr{name, err}
			}
		}(name, metaInfo)
	}
	wg.Wait()
	close(chMeta)
	close(chErr)

	if len(chErr) > 0 {
		ne := <-chErr
		combinedErr := errors.Errorf("[%s] %v", ne.name, ne.err.Error())
		for ne := range chErr {
			combinedErr = errors.Wrapf(combinedErr, "[%s] %v", ne.name, ne.err.Error())
		}
		return nil, combinedErr
	}

	result := map[string]interface{}{}
	for nm := range chMeta {
		result[nm.name] = nm.meta
	}
	return result, nil
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
