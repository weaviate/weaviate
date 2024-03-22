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

package batch

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

var _NUMCPU = runtime.GOMAXPROCS(0)

const BatchChannelSize = 100

type BatchJob struct {
	texts      []string
	tokens     []int
	ctx        context.Context
	wg         *sync.WaitGroup
	errs       map[int]error
	cfg        moduletools.ClassConfig
	vecs       [][]float32
	skipObject []bool
	startTime  time.Time
}

type BatchClient interface {
	Vectorize(ctx context.Context, input []string,
		config moduletools.ClassConfig) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error)
}

func NewBatchVectorizer(client BatchClient, maxBatchTime time.Duration, maxObjectsPerBatch int, maxTimePerVectorizerBatch float64, execAfterRequestFunction func(limits *modulecomponents.RateLimits), logger logrus.FieldLogger, needsInitialRequest bool) *Batch {
	batch := Batch{
		client:                    client,
		objectVectorizer:          objectsvectorizer.New(),
		jobQueueCh:                make(chan BatchJob, BatchChannelSize),
		maxBatchTime:              maxBatchTime,
		maxObjectsPerBatch:        maxObjectsPerBatch,
		maxTimePerVectorizerBatch: maxTimePerVectorizerBatch,
	}
	enterrors.GoWrapper(func() { batch.batchWorker(execAfterRequestFunction, needsInitialRequest, logger) }, logger)
	return &batch
}

type Batch struct {
	client                    BatchClient
	objectVectorizer          *objectsvectorizer.ObjectVectorizer
	jobQueueCh                chan BatchJob
	maxBatchTime              time.Duration
	maxObjectsPerBatch        int
	maxTimePerVectorizerBatch float64
}

// batchWorker is a go routine that handles the communication with the vectorizer
//
// On the high level it has the following steps:
//  1. It receives a batch job
//  2. It splits the job into smaller vectorizer-batches if the token limit is reached. Note that objects from different
//     batches are not mixed with each other to simplify returning the vectors.
//  3. It sends the smaller batches to the vectorizer
func (b *Batch) batchWorker(execAfterRequestFunction func(limits *modulecomponents.RateLimits), needsInitialRequest bool, logger logrus.FieldLogger) {
	rateLimit := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	rateLimit.ResetAfterRequestFunction()

	texts := make([]string, 0, 100)
	origIndex := make([]int, 0, 100)
	firstRequest := needsInitialRequest
	timePerToken := 0.0
	batchTookInS := float64(0)

	for job := range b.jobQueueCh {
		// the total batch should not take longer than 60s to avoid timeouts. We will only use 40s here to be safe

		objCounter := 0
		tokensInCurrentBatch := 0
		texts = texts[:0]
		origIndex = origIndex[:0]

		// we don't know the current rate limits without a request => send a small one
		for objCounter < len(job.texts) && firstRequest {
			var err error
			if !job.skipObject[objCounter] {
				err = b.makeRequest(job, job.texts[objCounter:objCounter+1], job.cfg, []int{objCounter}, rateLimit)
				if err != nil {
					job.errs[objCounter] = err
					continue
				}
				firstRequest = false
			}
			objCounter++
		}

		for objCounter < len(job.texts) {
			if job.ctx.Err() != nil {
				for j := objCounter; j < len(job.texts); j++ {
					if !job.skipObject[j] {
						job.errs[j] = fmt.Errorf("context deadline exceeded or cancelled")
					}
				}
				break
			}

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
			if float32(tokensInCurrentBatch+job.tokens[objCounter]) <= 0.95*float32(rateLimit.RemainingTokens) && (timePerToken*float64(tokensInCurrentBatch) < b.maxTimePerVectorizerBatch) && len(texts) < b.maxObjectsPerBatch {
				tokensInCurrentBatch += job.tokens[objCounter]
				texts = append(texts, text)
				origIndex = append(origIndex, objCounter)
				objCounter++
				if objCounter < len(job.texts) {
					continue
				}
			}

			// if a single object is larger than the current token limit we need to wait until the token limit refreshes
			// enough to be able to handle the object. This assumes that the tokenLimit refreshes linearly which is true
			// for openAI, but needs to be checked for other providers
			if len(texts) == 0 && rateLimit.ResetTokens > 0 {
				fractionOfTotalLimit := float32(job.tokens[objCounter]) / float32(rateLimit.LimitTokens)
				sleepTime := time.Duration(float32(rateLimit.ResetTokens)*fractionOfTotalLimit+1) * time.Second
				if time.Since(job.startTime)+sleepTime < b.maxBatchTime {
					time.Sleep(sleepTime)
					rateLimit.RemainingTokens += int(float32(rateLimit.LimitTokens) * fractionOfTotalLimit)
				} else {
					job.errs[objCounter] = fmt.Errorf("text too long for vectorization. Cannot wait for token refresh due to time limit")
					objCounter++
				}
				continue // try again or next item
			}

			start := time.Now()
			_ = b.makeRequest(job, texts, job.cfg, origIndex, rateLimit)
			batchTookInS = time.Since(start).Seconds()
			if tokensInCurrentBatch > 0 {
				timePerToken = batchTookInS / float64(tokensInCurrentBatch)
			}

			// not all request limits are included in "RemainingRequests" and "ResetRequests". For example, in the free
			// tier only the RPD limits are shown but not RPM
			if rateLimit.RemainingRequests == 0 && rateLimit.ResetRequests > 0 {
				// if we need to wait more than MaxBatchTime for a reset we need to stop the batch to not produce timeouts
				if time.Since(job.startTime)+time.Duration(rateLimit.ResetRequests)*time.Second > b.maxBatchTime {
					for j := origIndex[0]; j < len(job.texts); j++ {
						if !job.skipObject[j] {
							job.errs[j] = errors.New("request rate limit exceeded and will not refresh in time")
						}
					}
					break
				}
				time.Sleep(time.Duration(rateLimit.ResetRequests) * time.Second)
			}

			// reset for next vectorizer-batch
			tokensInCurrentBatch = 0
			texts = texts[:0]
			origIndex = origIndex[:0]
		}

		// in case we exit the loop without sending the last batch. This can happen when the last object is a skip or
		// is too long
		if len(texts) > 0 && objCounter == len(job.texts) {
			_ = b.makeRequest(job, texts, job.cfg, origIndex, rateLimit)
		}
		job.wg.Done()
	}
}

func (b *Batch) makeRequest(job BatchJob, texts []string, cfg moduletools.ClassConfig, origIndex []int, rateLimit *modulecomponents.RateLimits) error {
	res, rateLimitNew, err := b.client.Vectorize(job.ctx, texts, cfg)
	if err != nil {
		for j := 0; j < len(texts); j++ {
			job.errs[origIndex[j]] = err
		}
	} else {
		for j := 0; j < len(texts); j++ {
			if res.Errors != nil && res.Errors[j] != nil {
				job.errs[origIndex[j]] = res.Errors[j]
			} else {
				job.vecs[origIndex[j]] = res.Vector[j]
			}
		}
	}
	if rateLimitNew != nil {
		rateLimit.LimitRequests = rateLimitNew.LimitRequests
		rateLimit.LimitTokens = rateLimitNew.LimitTokens
		rateLimit.ResetRequests = rateLimitNew.ResetRequests
		rateLimit.ResetTokens = rateLimitNew.ResetTokens
		rateLimit.RemainingRequests = rateLimitNew.RemainingRequests
		rateLimit.RemainingTokens = rateLimitNew.RemainingTokens
	} else {
		rateLimit.ResetAfterRequestFunction()
	}

	return err
}

func (b *Batch) SubmitBatchAndWait(ctx context.Context, cfg moduletools.ClassConfig, skipObject []bool, tokenCounts []int, texts []string) ([][]float32, map[int]error) {
	vecs := make([][]float32, len(skipObject))
	errs := make(map[int]error)
	wg := sync.WaitGroup{}
	wg.Add(1)

	b.jobQueueCh <- BatchJob{
		ctx:        ctx,
		wg:         &wg,
		errs:       errs,
		cfg:        cfg,
		texts:      texts,
		tokens:     tokenCounts,
		vecs:       vecs,
		skipObject: skipObject,
		startTime:  time.Now(),
	}

	wg.Wait()
	return vecs, errs
}

type objectVectorizer func(context.Context, *models.Object, moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error)

func VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig, logger logrus.FieldLogger, objectVectorizer objectVectorizer) ([][]float32, []models.AdditionalProperties, map[int]error) {
	vecs := make([][]float32, len(objs))
	// error should be the exception so dont preallocate
	errs := make(map[int]error, 0)
	errorLock := sync.Mutex{}

	// error group is used to limit concurrency
	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(_NUMCPU * 2)
	for i := range objs {
		i := i

		if skipObject[i] {
			continue
		}
		eg.Go(func() error {
			vec, _, err := objectVectorizer(ctx, objs[i], cfg)
			if err != nil {
				errorLock.Lock()
				defer errorLock.Unlock()
				errs[i] = err
			}
			vecs[i] = vec
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		for i := range objs {
			if skipObject[i] {
				continue
			}
			errs[i] = err
		}
		return nil, nil, errs
	}
	return vecs, nil, errs
}
