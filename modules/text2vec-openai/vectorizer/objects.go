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

package vectorizer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

type batchJob struct {
	objects    []*models.Object
	texts      []string
	tokens     []int
	ctx        context.Context
	wg         *sync.WaitGroup
	errs       map[int]error
	cfg        moduletools.ClassConfig
	vecs       [][]float32
	skipObject []bool
}

type Vectorizer struct {
	client           Client
	objectVectorizer *objectsvectorizer.ObjectVectorizer
	jobQueueCh       chan batchJob
}

func New(client Client) *Vectorizer {
	vec := &Vectorizer{
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
		jobQueueCh:       make(chan batchJob),
	}

	go vec.batchWorker()
	return vec
}

type Client interface {
	Vectorize(ctx context.Context, input []string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, *ent.RateLimits, error)
	VectorizeQuery(ctx context.Context, input []string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Model() string
	Type() string
	ModelVersion() string
	ResourceName() string
	DeploymentID() string
	BaseURL() string
	IsAzure() bool
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, error) {
	text := v.objectVectorizer.Texts(ctx, object, NewClassSettings(cfg))
	res, _, err := v.client.Vectorize(ctx, []string{text}, v.getVectorizationConfig(cfg))
	if err != nil {
		return nil, err
	}

	if len(res.Vector) > 1 {
		return libvectorizer.CombineVectors(res.Vector), nil
	}
	return res.Vector[0], nil
}

func (v *Vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	settings := NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Type:         settings.Type(),
		Model:        settings.Model(),
		ModelVersion: settings.ModelVersion(),
		ResourceName: settings.ResourceName(),
		DeploymentID: settings.DeploymentID(),
		BaseURL:      settings.BaseURL(),
		IsAzure:      settings.IsAzure(),
		Dimensions:   settings.Dimensions(),
	}
}

// batchWorker is a go routine that handles the communication with the vectorizer
//
// On the high level it has the following steps:
//  1. It receives a batch job
//  2. It splits the job into smaller vectorizer-batches if the token limit is reached. Note that objects from different
//     batches are not mixed with each other.
//  3. It sends the smaller batches to the vectorizer
func (v *Vectorizer) batchWorker() {
	tokenLimit := 1000 // don't know limits before the first request
	remainingRequests := 10
	requestLimitReset := 0
	texts := make([]string, 0, 100)
	for job := range v.jobQueueCh {
		conf := v.getVectorizationConfig(job.cfg)
		objCounter := 0
		vecBatchOffset := 0 // index of first object in current vectorizer-batch

		tokensInCurrentBatch := 0
		texts = texts[:0]
		for {
			if job.skipObject[objCounter] {
				objCounter++
				if objCounter == len(job.objects) {
					break
				}
				continue
			}

			// add input to vectorizer-batches until current token limit is reached
			text := job.texts[objCounter]
			tokensInCurrentBatch += job.tokens[objCounter]
			if float32(tokensInCurrentBatch) < 0.95*float32(tokenLimit) {
				texts = append(texts, text)
				if objCounter < len(job.objects)-1 {
					objCounter++
					continue
				}
			}

			if job.ctx.Err() != nil {
				for j := 0; j < len(texts); j++ {
					job.errs[vecBatchOffset+j] = fmt.Errorf("context deadline exceeded or cancelled")
				}
				break
			}

			res, rateLimit, err := v.client.Vectorize(job.ctx, texts, conf)
			if err != nil {
				for j := 0; j < len(texts); j++ {
					job.errs[vecBatchOffset+j] = err
				}
			} else {
				for j := 0; j < len(texts); j++ {
					if res.Errors[j] != nil {
						job.errs[vecBatchOffset+j] = res.Errors[j]
					} else {
						job.vecs[vecBatchOffset+j] = res.Vector[j]
					}
				}
			}

			if rateLimit != nil {
				tokenLimit = rateLimit.RemainingTokens
				remainingRequests = rateLimit.RemainingRequests
				requestLimitReset = rateLimit.ResetRequests
			}

			// not all request limits are included in "RemainingRequests" and "ResetRequests". For example, in the free
			// tier only the RPD limits are shown but not RPM
			if remainingRequests == 0 {
				// if we need to wait more than 60s for a reset we need to stop the batch to not produce timeouts
				if requestLimitReset >= 61 {
					for j := vecBatchOffset; j < len(job.objects); j++ {
						job.errs[j] = err
					}
					break
				}
				time.Sleep(time.Duration(requestLimitReset) * time.Second)
			}

			objCounter++
			if objCounter == len(job.objects) {
				break
			}

			// reset for next vectorizer-batch
			vecBatchOffset = objCounter
			tokensInCurrentBatch = 0
			texts = texts[:0]
		}
		job.wg.Done()

	}
}

func (v *Vectorizer) ObjectBatch(ctx context.Context, objects []*models.Object, skipObject []bool, cfg moduletools.ClassConfig,
) ([][]float32, map[int]error) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	errs := make(map[int]error)
	texts := make([]string, len(objects))
	tokens := make([]int, len(objects))
	conf := v.getVectorizationConfig(cfg)
	icheck := NewClassSettings(cfg)
	vecs := make([][]float32, len(objects))

	// prepare input for vectorizer, and send it to the queue. Prepare here to avoid work in the queue-worker
	for i := range objects {
		if skipObject[i] {
			continue
		}
		text := v.objectVectorizer.Texts(ctx, objects[i], icheck)
		count, _ := clients.GetTokensCount(conf.Model, text)
		texts[i] = text
		tokens[i] = count
	}

	v.jobQueueCh <- batchJob{objects: objects, ctx: ctx, wg: &wg, errs: errs, cfg: cfg, texts: texts, tokens: tokens, vecs: vecs, skipObject: skipObject}

	wg.Wait()

	return vecs, errs
}
