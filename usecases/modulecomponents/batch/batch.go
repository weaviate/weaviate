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
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/monitoring"

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
	apiKeyHash [32]byte
	tokenSum   int
}

func (b BatchJob) copy() BatchJob {
	return BatchJob{
		texts:      b.texts,
		tokens:     b.tokens,
		ctx:        b.ctx,
		wg:         b.wg,
		errs:       b.errs,
		cfg:        b.cfg,
		vecs:       b.vecs,
		skipObject: b.skipObject,
		startTime:  b.startTime,
		apiKeyHash: b.apiKeyHash,
		tokenSum:   b.tokenSum,
	}
}

type BatchClient interface {
	Vectorize(ctx context.Context, input []string,
		config moduletools.ClassConfig) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error)
	GetVectorizerRateLimit(ctx context.Context, config moduletools.ClassConfig) *modulecomponents.RateLimits
	GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte
}

func NewBatchVectorizer(client BatchClient, maxBatchTime time.Duration, maxObjectsPerBatch int, maxTokensPerBatch func(cfg moduletools.ClassConfig) int, maxTimePerVectorizerBatch float64, logger logrus.FieldLogger, label string) *Batch {
	batch := Batch{
		client:                    client,
		objectVectorizer:          objectsvectorizer.New(),
		jobQueueCh:                make(chan BatchJob, BatchChannelSize),
		maxBatchTime:              maxBatchTime,
		maxObjectsPerBatch:        maxObjectsPerBatch,
		maxTimePerVectorizerBatch: maxTimePerVectorizerBatch,
		maxTokensPerBatch:         maxTokensPerBatch,
		concurrentBatches:         atomic.Int32{},
		logger:                    logger,
		label:                     label,
	}

	batch.rateLimitChannel = make(chan rateLimitJob, BatchChannelSize)
	batch.endOfBatchChannel = make(chan endOfBatchJob, BatchChannelSize)

	enterrors.GoWrapper(func() { batch.batchWorker() }, logger)
	return &batch
}

type rateLimitJob struct {
	rateLimit  *modulecomponents.RateLimits
	apiKeyHash [32]byte
}

type endOfBatchJob struct {
	timePerToken      float64
	objectsPerRequest int
	reservedTokens    int
	reservedReqs      int
	apiKeyHash        [32]byte
	concurrentBatch   bool
}

type Batch struct {
	client                    BatchClient
	objectVectorizer          *objectsvectorizer.ObjectVectorizer
	jobQueueCh                chan BatchJob
	maxBatchTime              time.Duration
	maxObjectsPerBatch        int
	maxTimePerVectorizerBatch float64
	maxTokensPerBatch         func(cfg moduletools.ClassConfig) int
	rateLimitChannel          chan rateLimitJob
	endOfBatchChannel         chan endOfBatchJob
	concurrentBatches         atomic.Int32
	logger                    logrus.FieldLogger
	label                     string
}

// batchWorker is a go routine that handles the communication with the vectorizer
//
// On the high level it has the following steps:
//  1. It receives a batch job
//  2. It splits the job into smaller vectorizer-batches if the token limit is reached. Note that objects from different
//     batches are not mixed with each other to simplify returning the vectors.
//  3. It sends the smaller batches to the vectorizer
func (b *Batch) batchWorker() {
	timePerToken := 0.0
	objectsPerBatch := b.maxObjectsPerBatch

	rateLimitPerApiKey := make(map[[32]byte]*modulecomponents.RateLimits)

	// the total batch should not take longer than 60s to avoid timeouts. We will only use 40s here to be safe
	for job := range b.jobQueueCh {
		// observe how long the batch was in the queue waiting for processing
		durWaiting := time.Since(job.startTime).Seconds()
		monitoring.GetMetrics().T2VBatchQueueDuration.WithLabelValues(b.label, "waiting_for_processing").
			Observe(durWaiting)

		startProcessingTime := time.Now()

		// check if we already have rate limits for the current api key and reuse them if possible
		rateLimit, ok := rateLimitPerApiKey[job.apiKeyHash]
		if !ok {
			rateLimit = b.client.GetVectorizerRateLimit(job.ctx, job.cfg)
			rateLimitPerApiKey[job.apiKeyHash] = rateLimit
		}
		rateLimit.CheckForReset()

		objCounter := 0

		// If the user does not supply rate limits, and we do not have defaults for the provider we don't know the
		// rate limits without a request => send a small one. This currently only affects OpenAI.
		for objCounter < len(job.texts) && rateLimit.IsInitialized() {
			var err error
			if !job.skipObject[objCounter] {
				err = b.makeRequest(job, job.texts[objCounter:objCounter+1], job.cfg, []int{objCounter}, rateLimit, job.tokens[objCounter])
				if err != nil {
					job.errs[objCounter] = err
					objCounter++
					continue
				}
			}
			objCounter++
		}

		// if we have a high rate limit we can send multiple batches in parallel.
		//
		// If the rate limit is high enough to "fit" the current batch, we send it concurrently. If not, we wait for
		// either
		//   - the rate limit to refresh, so we can schedule another concurrent batch
		//   - the current batch to finish, so the next batch can be sent sequentially
		//
		// While using the same code both modes are working slightly different:
		// 	- For concurrent batching, the amount of used tokens/requests is reserved as long as the batch is running
		//	  and is cleared when it finishes. This ensures that we never exceed the rate limit and don't need to check
		//	  the rate limit in the sendBatch function (we use a dummy that never fails a check). All updates happen
		//	  in the main loop.
		//  - For sequential batching, the rate limit  will be passed into the sendBatch function and is observed and
		//    updated there. This allows to use the rate-limit in an optimal way, but also requires more checks. No
		//    concurrent batch can be started while a sequential batch is running.
		repeats := 0
		for {
			timePerToken, objectsPerBatch = b.updateState(rateLimitPerApiKey, timePerToken, objectsPerBatch)
			numRequests := 1 + int(1.25*float32(len(job.texts)))/objectsPerBatch // round up to be on the safe side

			stats := monitoring.GetMetrics().T2VRateLimitStats
			stats.WithLabelValues(b.label, "token_limit").Set(float64(rateLimit.LimitTokens))
			stats.WithLabelValues(b.label, "token_remaining").Set(float64(rateLimit.RemainingTokens))
			stats.WithLabelValues(b.label, "token_reserved").Set(float64(rateLimit.ReservedTokens))
			stats.WithLabelValues(b.label, "request_limit").Set(float64(rateLimit.LimitRequests))
			stats.WithLabelValues(b.label, "request_remaining").Set(float64(rateLimit.RemainingRequests))
			stats.WithLabelValues(b.label, "request_reserved").Set(float64(rateLimit.ReservedRequests))
			stats.WithLabelValues(b.label, "estimated_requests_needed").Set(float64(numRequests))
			stats.WithLabelValues(b.label, "tokens_needed").Set(float64(job.tokenSum))
			stats.WithLabelValues(b.label, "concurrent_batches").Set(float64(b.concurrentBatches.Load()))
			stats.WithLabelValues(b.label, "repeats_for_scheduling").Set(float64(repeats))

			if rateLimit.CanSendFullBatch(numRequests, job.tokenSum) {
				b.concurrentBatches.Add(1)
				monitoring.GetMetrics().T2VBatches.WithLabelValues(b.label).Inc()
				jobCopy := job.copy()
				rateLimit.ReservedRequests += numRequests
				rateLimit.ReservedTokens += job.tokenSum

				// necessary, because the outer loop can modify these values through b.updateState while the goroutine
				// is accessing them => race
				timePerToken := timePerToken
				numRequests := numRequests
				enterrors.GoWrapper(func() {
					b.sendBatch(jobCopy, objCounter, dummyRateLimit(), timePerToken, numRequests, true)
					monitoring.GetMetrics().T2VBatchQueueDuration.WithLabelValues(b.label, "processing_async").
						Observe(time.Since(startProcessingTime).Seconds())
				}, b.logger)
				break
			} else if b.concurrentBatches.Load() < 1 {
				b.concurrentBatches.Add(1)
				monitoring.GetMetrics().T2VBatches.WithLabelValues(b.label).Inc()
				// block so no concurrent batch can be sent
				b.sendBatch(job, objCounter, rateLimit, timePerToken, 0, false)
				monitoring.GetMetrics().T2VBatchQueueDuration.WithLabelValues(b.label, "processing_sync").
					Observe(time.Since(startProcessingTime).Seconds())
				break
			}
			time.Sleep(100 * time.Millisecond)
			repeats++
		}
	}
}

// updateState collects the latest updates from finished batches
func (b *Batch) updateState(rateLimits map[[32]byte]*modulecomponents.RateLimits, timePerToken float64, objectsPerBatch int) (float64, int) {
	for _, rateLimit := range rateLimits {
		rateLimit.CheckForReset()
	}

	// read all values from the channel and only keep the freshest one. This is needed as openAI returns the current
	// rate limit with every request, and we need to keep the freshest one to get an overview of where we are
rateLimitLoop:
	for {
		select {
		case rateLimitEntry := <-b.rateLimitChannel:
			rateLimits[rateLimitEntry.apiKeyHash] = rateLimitEntry.rateLimit
		default:
			break rateLimitLoop
		}
	}

timeLoop:
	for {
		select {
		case endOfBatch := <-b.endOfBatchChannel:
			timePerToken = endOfBatch.timePerToken
			if endOfBatch.objectsPerRequest > 0 {
				objectsPerBatch = endOfBatch.objectsPerRequest
			}

			// if we have a concurrent batch we need to remove the reserved tokens from the rate limit
			if endOfBatch.concurrentBatch {
				rateLimits[endOfBatch.apiKeyHash].ReservedTokens -= endOfBatch.reservedTokens
				rateLimits[endOfBatch.apiKeyHash].ReservedRequests -= endOfBatch.reservedReqs
			}

		default:
			break timeLoop
		}
	}
	return timePerToken, objectsPerBatch
}

func (b *Batch) sendBatch(job BatchJob, objCounter int, rateLimit *modulecomponents.RateLimits, timePerToken float64, reservedReqs int, concurrentBatch bool) {
	maxTokensPerBatch := b.maxTokensPerBatch(job.cfg)
	tokensInCurrentBatch := 0
	numRequests := 0
	numSendObjects := 0

	texts := make([]string, 0, 100)
	origIndex := make([]int, 0, 100)

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

		// add objects to the current vectorizer-batch until the remaining tokens are used up or other limits are reached
		text := job.texts[objCounter]
		if float32(tokensInCurrentBatch+job.tokens[objCounter]) <= 0.95*float32(rateLimit.RemainingTokens) &&
			float32(tokensInCurrentBatch+job.tokens[objCounter]) <= 0.95*float32(maxTokensPerBatch) &&
			(timePerToken*float64(tokensInCurrentBatch) < b.maxTimePerVectorizerBatch) &&
			len(texts) < b.maxObjectsPerBatch {
			tokensInCurrentBatch += job.tokens[objCounter]
			texts = append(texts, text)
			origIndex = append(origIndex, objCounter)
			objCounter++
			if objCounter < len(job.texts) {
				continue
			}
		}

		// if a single object is larger than the current token limit it will fail all tests above. Then we need to either
		//   - wait until the token limit refreshes. This assumes that the tokenLimit refreshes linearly which is true
		//     for openAI, but needs to be checked for other providers
		//   - send it anyway and let the provider fail it
		if len(texts) == 0 {
			fractionOfTotalLimit := float64(job.tokens[objCounter]) / float64(rateLimit.LimitTokens)
			sleepTime := time.Duration(fractionOfTotalLimit * float64(time.Until(rateLimit.ResetTokens)))
			// Only sleep if values are reasonable, e.g. for the token counter is lower than the limit token and we do
			// not blow up the sleep time
			if sleepTime > 0 && fractionOfTotalLimit < 1 && time.Since(job.startTime)+sleepTime < b.maxBatchTime && !concurrentBatch {
				time.Sleep(sleepTime)
				rateLimit.RemainingTokens += int(float64(rateLimit.LimitTokens) * fractionOfTotalLimit)
				continue // try again after tokens have hopefully refreshed
			} else {
				// send the item in an individual request even if it is larger than the absolute token limit. It needs
				// to fail to propagate the proper error to the user - also our tokenCounts are approximations so even if
				// an objects seems to be too big it might as well work
				texts = append(texts, text)
				origIndex = append(origIndex, objCounter)
				objCounter++
			}
		}

		start := time.Now()
		_ = b.makeRequest(job, texts, job.cfg, origIndex, rateLimit, tokensInCurrentBatch)
		batchTookInS := time.Since(start).Seconds()
		if tokensInCurrentBatch > 0 {
			timePerToken = batchTookInS / float64(tokensInCurrentBatch)
		}
		numRequests += 1
		numSendObjects += len(texts)

		// in case of low rate limits we should not send the next batch immediately but sleep a bit
		batchesPerMinute := 61.0 / batchTookInS
		if batchesPerMinute > float64(rateLimit.LimitRequests) {
			sleepFor := time.Duration((60.0-batchTookInS*float64(rateLimit.LimitRequests))/float64(rateLimit.LimitRequests)) * time.Second
			time.Sleep(sleepFor)

			// adapt the batches per limit
			batchesPerMinute = float64(rateLimit.LimitRequests)
		}
		if batchesPerMinute*float64(tokensInCurrentBatch) > float64(rateLimit.LimitTokens) {
			sleepFor := batchTookInS * (batchesPerMinute*float64(tokensInCurrentBatch) - float64(rateLimit.LimitTokens)) / float64(rateLimit.LimitTokens)
			time.Sleep(time.Duration(sleepFor * float64(time.Second)))
		}

		// not all request limits are included in "RemainingRequests" and "ResetRequests". For example, in the OpenAI
		// free tier only the RPD limits are shown but not RPM
		if rateLimit.RemainingRequests <= 0 && time.Until(rateLimit.ResetRequests) > 0 {
			// if we need to wait more than MaxBatchTime for a reset we need to stop the batch to not produce timeouts
			if time.Since(job.startTime)+time.Until(rateLimit.ResetRequests) > b.maxBatchTime {
				for j := origIndex[0]; j < len(job.texts); j++ {
					if !job.skipObject[j] {
						job.errs[j] = errors.New("request rate limit exceeded and will not refresh in time")
					}
				}
				break
			}
			time.Sleep(time.Until(rateLimit.ResetRequests))
		}

		// reset for next vectorizer-batch
		tokensInCurrentBatch = 0
		texts = texts[:0]
		origIndex = origIndex[:0]
	}

	// in case we exit the loop without sending the last batch. This can happen when the last object is a skip or
	// is too long
	if len(texts) > 0 && objCounter == len(job.texts) {
		_ = b.makeRequest(job, texts, job.cfg, origIndex, rateLimit, tokensInCurrentBatch)
	}
	objectsPerRequest := 0
	if numRequests > 0 {
		objectsPerRequest = numSendObjects / numRequests
	}
	monitoring.GetMetrics().T2VRequestsPerBatch.WithLabelValues(b.label).Observe(float64(numRequests))
	b.endOfBatchChannel <- endOfBatchJob{
		timePerToken:      timePerToken,
		objectsPerRequest: objectsPerRequest,
		reservedTokens:    job.tokenSum,
		reservedReqs:      reservedReqs,
		apiKeyHash:        job.apiKeyHash,
		concurrentBatch:   concurrentBatch,
	}
	job.wg.Done()
	b.concurrentBatches.Add(-1)
	monitoring.GetMetrics().T2VBatches.WithLabelValues(b.label).Dec()
}

func (b *Batch) makeRequest(job BatchJob, texts []string, cfg moduletools.ClassConfig, origIndex []int, rateLimit *modulecomponents.RateLimits, tokensInCurrentBatch int) error {
	beforeRequest := time.Now()
	defer func() {
		monitoring.GetMetrics().T2VRequestDuration.WithLabelValues(b.label).
			Observe(time.Since(beforeRequest).Seconds())
	}()

	monitoring.GetMetrics().T2VTokensInRequest.WithLabelValues(b.label).
		Observe(float64(tokensInCurrentBatch))

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
		b.rateLimitChannel <- rateLimitJob{rateLimit: rateLimitNew, apiKeyHash: job.apiKeyHash}
	} else {
		rateLimit.ResetAfterRequestFunction(tokensInCurrentBatch)
	}

	return err
}

func (b *Batch) SubmitBatchAndWait(ctx context.Context, cfg moduletools.ClassConfig, skipObject []bool, tokenCounts []int, texts []string) ([][]float32, map[int]error) {
	vecs := make([][]float32, len(skipObject))
	errs := make(map[int]error)
	wg := sync.WaitGroup{}
	wg.Add(1)

	tokenSum := 0
	for i := range tokenCounts {
		tokenSum += tokenCounts[i]
	}

	monitoring.GetMetrics().T2VTokensInBatch.WithLabelValues(b.label).
		Observe(float64(tokenSum))

	beforeEnqueue := time.Now()
	b.jobQueueCh <- BatchJob{
		ctx:        ctx,
		wg:         &wg,
		errs:       errs,
		cfg:        cfg,
		texts:      texts,
		tokens:     tokenCounts,
		vecs:       vecs,
		skipObject: skipObject,
		apiKeyHash: b.client.GetApiKeyHash(ctx, cfg),
		startTime:  time.Now(),
		tokenSum:   tokenSum,
	}

	// observe enqueue duration
	monitoring.GetMetrics().T2VBatchQueueDuration.WithLabelValues(b.label, "enqueue").
		Observe(time.Since(beforeEnqueue).Seconds())

	wg.Wait()

	// observe total duration
	monitoring.GetMetrics().T2VBatchQueueDuration.WithLabelValues(b.label, "total").
		Observe(time.Since(beforeEnqueue).Seconds())
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

func dummyRateLimit() *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{
		LimitRequests:        1000000,
		LimitTokens:          1000000,
		RemainingRequests:    1000000,
		RemainingTokens:      1000000,
		ResetRequests:        time.Now(),
		ResetTokens:          time.Now(),
		AfterRequestFunction: func(limits *modulecomponents.RateLimits, tokensUsed int, deductRequest bool) {},
	}
}
