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
	"fmt"
	"strings"
	"sync"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/transformers"

	"github.com/pkg/errors"
)

func (v *vectorizer) MetaInfo() (map[string]any, error) {
	type nameMetaErr struct {
		name string
		meta map[string]any
		err  error
	}

	endpoints := map[string]string{}
	if v.originPassage != v.originQuery {
		endpoints["passage"] = v.urlBuilder.GetPassageURL("/meta", transformers.VectorizationConfig{})
		endpoints["query"] = v.urlBuilder.GetQueryURL("/meta", transformers.VectorizationConfig{})
	} else {
		endpoints[""] = v.urlBuilder.GetPassageURL("/meta", transformers.VectorizationConfig{})
	}

	var wg sync.WaitGroup
	ch := make(chan nameMetaErr, len(endpoints))
	for serviceName, endpoint := range endpoints {
		serviceName, endpoint := serviceName, endpoint
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			meta, err := v.client.MetaInfo(endpoint)
			ch <- nameMetaErr{serviceName, meta, err}
		}, v.logger)
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
		return nil, errors.New(strings.Join(errs, ", "))
	}
	if len(metas) == 1 {
		for _, meta := range metas {
			return meta.(map[string]any), nil
		}
	}
	return metas, nil
}
