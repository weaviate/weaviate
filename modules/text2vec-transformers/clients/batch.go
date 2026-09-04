//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	txvectorizer "github.com/weaviate/weaviate/modules/text2vec-transformers/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/transformers"
)

const (
	readyEndpoint = "/.well-known/ready"
	metaEndpoint  = "/meta"
)

// BatchVectorizer implements batchtext.Client interface for batch vectorization.
// This is a separate struct to avoid method signature conflicts with the legacy vectorizer.
type BatchVectorizer struct {
	originPassage string
	originQuery   string
	client        *transformers.Client
	urlBuilder    *transformers.URLBuilder
	logger        logrus.FieldLogger
}

// NewBatchVectorizer creates a new batch-capable vectorizer client.
func NewBatchVectorizer(originPassage, originQuery string, timeout time.Duration, logger logrus.FieldLogger) *BatchVectorizer {
	urlBuilder := transformers.NewURLBuilder(originPassage, originQuery)
	return &BatchVectorizer{
		originPassage: originPassage,
		originQuery:   originQuery,
		urlBuilder:    urlBuilder,
		client:        transformers.New(urlBuilder, timeout, logger),
		logger:        logger,
	}
}

// Vectorize implements batchtext.Client interface.
// Sends all input texts in a single HTTP request for passage/document vectorization.
func (b *BatchVectorizer) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	config := b.getVectorizationConfig(cfg)
	vectors, err := b.client.VectorizeObjects(ctx, input, config)
	if err != nil {
		return nil, nil, 0, err
	}
	return &modulecomponents.VectorizationResult[[]float32]{
		Text:   input,
		Vector: vectors,
	}, nil, 0, nil
}

// VectorizeQuery implements batchtext.Client interface.
// Sends all input texts in a single HTTP request for query vectorization.
func (b *BatchVectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	config := b.getVectorizationConfig(cfg)
	vectors, err := b.client.VectorizeQueries(ctx, input, config)
	if err != nil {
		return nil, err
	}
	return &modulecomponents.VectorizationResult[[]float32]{
		Text:   input,
		Vector: vectors,
	}, nil
}

// getVectorizationConfig extracts the transformers config from ClassConfig.
func (b *BatchVectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) transformers.VectorizationConfig {
	icheck := txvectorizer.NewClassSettings(cfg)
	return transformers.VectorizationConfig{
		PoolingStrategy:     icheck.PoolingStrategy(),
		InferenceURL:        icheck.InferenceURL(),
		PassageInferenceURL: icheck.PassageInferenceURL(),
		QueryInferenceURL:   icheck.QueryInferenceURL(),
		Dimensions:          icheck.Dimensions(),
	}
}

// WaitForStartup waits for the inference server(s) to be ready.
func (b *BatchVectorizer) WaitForStartup(initCtx context.Context, interval time.Duration) error {
	endpoints := map[string]string{}
	if b.originPassage != b.originQuery {
		endpoints["passage"] = b.urlBuilder.GetPassageURL(readyEndpoint, transformers.VectorizationConfig{})
		endpoints["query"] = b.urlBuilder.GetQueryURL(readyEndpoint, transformers.VectorizationConfig{})
	} else {
		endpoints[""] = b.urlBuilder.GetPassageURL(readyEndpoint, transformers.VectorizationConfig{})
	}

	ch := make(chan error, len(endpoints))
	var wg sync.WaitGroup
	for serviceName, endpoint := range endpoints {
		serviceName, endpoint := serviceName, endpoint
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			if err := b.waitFor(initCtx, interval, endpoint, serviceName); err != nil {
				ch <- err
			}
		}, b.logger)
	}
	wg.Wait()
	close(ch)

	if len(ch) > 0 {
		var errs []string
		for err := range ch {
			errs = append(errs, err.Error())
		}
		return errors.New(strings.Join(errs, ", "))
	}
	return nil
}

func (b *BatchVectorizer) waitFor(initCtx context.Context, interval time.Duration, endpoint string, serviceName string) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	expired := initCtx.Done()
	var lastErr error
	prefix := ""
	if serviceName != "" {
		prefix = "[" + serviceName + "] "
	}

	for {
		select {
		case <-ticker.C:
			lastErr = b.client.CheckReady(initCtx, endpoint)
			if lastErr == nil {
				return nil
			}
			b.logger.
				WithField("action", "transformer_remote_wait_for_startup").
				WithError(lastErr).Warnf("%stransformer remote inference service not ready", prefix)
		case <-expired:
			return errors.Wrapf(lastErr, "%sinit context expired before remote was ready", prefix)
		}
	}
}

// MetaInfo returns metadata about the inference server(s).
func (b *BatchVectorizer) MetaInfo() (map[string]interface{}, error) {
	type nameMetaErr struct {
		name string
		meta map[string]any
		err  error
	}

	endpoints := map[string]string{}
	if b.originPassage != b.originQuery {
		endpoints["passage"] = b.urlBuilder.GetPassageURL(metaEndpoint, transformers.VectorizationConfig{})
		endpoints["query"] = b.urlBuilder.GetQueryURL(metaEndpoint, transformers.VectorizationConfig{})
	} else {
		endpoints[""] = b.urlBuilder.GetPassageURL(metaEndpoint, transformers.VectorizationConfig{})
	}

	var wg sync.WaitGroup
	ch := make(chan nameMetaErr, len(endpoints))
	for serviceName, endpoint := range endpoints {
		serviceName, endpoint := serviceName, endpoint
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			meta, err := b.client.MetaInfo(endpoint)
			ch <- nameMetaErr{serviceName, meta, err}
		}, b.logger)
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
