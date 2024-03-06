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

	"github.com/pkoukk/tiktoken-go"

	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

const (
	MaxBatchTime       = 40 * time.Second
	MaxObjectsPerBatch = 2000 // https://platform.openai.com/docs/api-reference/embeddings/create
)

type batchJob struct {
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
//     batches are not mixed with each other to simplify returning the vectors.
//  3. It sends the smaller batches to the vectorizer
func (v *Vectorizer) batchWorker() {
	rateLimit := &ent.RateLimits{}
	texts := make([]string, 0, 100)
	firstRequest := true
	timePerToken := 0.0
	batchTookInS := float64(0)
BatchLoop:
	for job := range v.jobQueueCh {
		// the total batch should not take longer than 60s to avoid timeouts. We will only use 40s here to be safe
		batchStart := time.Now()

		objCounter := 0
		vecBatchOffset := 0 // index of first object in current vectorizer-batch
		tokensInCurrentBatch := 0
		texts = texts[:0]

		conf := v.getVectorizationConfig(job.cfg)

		// we don't know the current rate limits without a request => send a small one
		for firstRequest {
			var err error
			rateLimit, err = v.makeRequest(job, job.texts[:1], conf, vecBatchOffset)
			if err != nil {
				for j := 0; j < len(job.texts); j++ {
					job.errs[j] = err
				}
				job.wg.Done()
				continue BatchLoop
			}
			objCounter++
			vecBatchOffset++
			firstRequest = false
		}

		for objCounter < len(job.texts) {
			if job.skipObject[objCounter] {
				objCounter++
				continue
			}

			if job.tokens[objCounter] > rateLimit.LimitTokens {
				job.errs[objCounter] = fmt.Errorf("text too long for vectorization")
				objCounter++
				continue
			}

			// add objects to the current vectorizer-batch until the remaining tokens are used up or other limits are reached
			text := job.texts[objCounter]
			if float32(tokensInCurrentBatch+job.tokens[objCounter]) < 0.95*float32(rateLimit.RemainingTokens) && (timePerToken*float64(tokensInCurrentBatch) < MaxBatchTime.Seconds()/4) && len(texts) < MaxObjectsPerBatch {
				tokensInCurrentBatch += job.tokens[objCounter]
				texts = append(texts, text)
				objCounter++
				if objCounter < len(job.texts) {
					continue
				}
			}

			// if a single object is larger than the current token limit we need to wait until the token limit refreshes
			// enough to be able to handle the object. This assumes that the tokenLimit refreshes linearly which is true
			// for openAI, but needs to be checked for other providers
			if len(texts) == 0 {
				fractionOfTotalLimit := float32(rateLimit.LimitTokens) / float32(job.tokens[objCounter])
				sleepTime := time.Duration(float32(rateLimit.ResetTokens)*fractionOfTotalLimit+1) * time.Second
				if time.Since(batchStart)+sleepTime > MaxBatchTime {
					time.Sleep(sleepTime)
					rateLimit.RemainingTokens += int(float32(rateLimit.LimitTokens) * fractionOfTotalLimit)
				} else {
					job.errs[objCounter] = fmt.Errorf("text too long for vectorization. Cannot wait for token refresh due to time limit")
					objCounter++
				}
				continue // try again or next item
			}

			if job.ctx.Err() != nil {
				for j := 0; j < len(texts); j++ {
					job.errs[vecBatchOffset+j] = fmt.Errorf("context deadline exceeded or cancelled")
				}
				break
			}
			start := time.Now()
			rateLimitNew, err := v.makeRequest(job, texts, conf, vecBatchOffset)
			batchTookInS = time.Since(start).Seconds()
			timePerToken = batchTookInS / float64(tokensInCurrentBatch)
			if rateLimitNew != nil {
				rateLimit = rateLimitNew
			}
			// not all request limits are included in "RemainingRequests" and "ResetRequests". For example, in the free
			// tier only the RPD limits are shown but not RPM
			if rateLimit.RemainingRequests == 0 {
				// if we need to wait more than MaxBatchTime for a reset we need to stop the batch to not produce timeouts
				if time.Since(batchStart)+time.Duration(rateLimit.ResetRequests)*time.Second > MaxBatchTime {
					for j := vecBatchOffset; j < len(job.texts); j++ {
						job.errs[j] = err
					}
					break
				}
				time.Sleep(time.Duration(rateLimit.ResetRequests) * time.Second)
			}

			// reset for next vectorizer-batch
			vecBatchOffset = objCounter
			tokensInCurrentBatch = 0
			texts = texts[:0]
		}
		job.wg.Done()

	}
}

func (v *Vectorizer) makeRequest(job batchJob, texts []string, conf ent.VectorizationConfig, vecBatchOffset int,
) (*ent.RateLimits, error) {
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

	return rateLimit, err
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

	// go token library is outdated. Alter the model-name to use a different model name with the same tokenization-behaviour
	model := conf.Model
	if model == "text-embedding-ada-002" || model == "text-embedding-3-small" || model == "text-embedding-3-large" {
		model = "gpt-4"
	}

	tke, err := tiktoken.EncodingForModel(model)
	if err != nil { // fail all objects as they all have the same model
		for j := range objects {
			errs[j] = err
		}
		return nil, errs
	}

	// prepare input for vectorizer, and send it to the queue. Prepare here to avoid work in the queue-worker
	for i := range objects {
		if skipObject[i] {
			continue
		}
		text := v.objectVectorizer.Texts(ctx, objects[i], icheck)
		texts[i] = text
		tokens[i] = clients.GetTokensCount(conf.Model, text, tke)
	}

	v.jobQueueCh <- batchJob{ctx: ctx, wg: &wg, errs: errs, cfg: cfg, texts: texts, tokens: tokens, vecs: vecs, skipObject: skipObject}

	wg.Wait()

	return vecs, errs
}
