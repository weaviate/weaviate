//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-jev/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const (
	apiPath     = "/v1/systemone"
	questionKey = "q"
	noulType    = "noul"
	scoreType   = "score"

	// DefaultMaxConcurrentRequests and MaxConcurrentRequestsLimit bound the
	// requests to the Jev API that are in flight across all queries of the
	// process.
	DefaultMaxConcurrentRequests = 16
	MaxConcurrentRequestsLimit   = 256

	maxAttempts = 4
	// maxRetryAfter caps how long a Retry-After header can delay a retry.
	maxRetryAfter = 10 * time.Second
	// cacheCapacity bounds the memory of the judgment cache. An entry is a
	// fixed-size hash and a float, so 100,000 entries stay below 20 MB.
	cacheCapacity = 100_000
	// maxErrorBodyBytes bounds how much of an error response ends up in the
	// error message.
	maxErrorBodyBytes = 512
)

type client struct {
	apiKey       string
	httpClient   *http.Client
	retryBackoff time.Duration
	cache        *judgmentCache
	// inFlight holds one token per request being sent. It is shared by all
	// Rank calls, so it limits the load on the Jev API however many queries
	// run at once.
	inFlight chan struct{}
	logger   logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, maxConcurrentRequests int, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:       apiKey,
		httpClient:   modulecomponents.NewBaseHttpClient(timeout),
		retryBackoff: 500 * time.Millisecond,
		cache:        newJudgmentCache(cacheCapacity),
		inFlight:     make(chan struct{}, maxConcurrentRequests),
		logger:       logger,
	}
}

// Rank asks Jev about each document and returns the answer as the score. By
// default the query is a condition and the answer is the probability that it
// is true. With a rubric (config.ScoreLevels) the query is a question of
// degree and the answer is the document's position on the rubric, from 0 for
// the first level.
//
// Documents are sent in batches of the class's batchSize. One request costs a
// fixed number of tokens on top of its documents, so batches are cheaper and
// need fewer requests, but the probability of a document then depends on the
// documents it shares the request with. Identical documents and documents
// judged before are not sent again. Requests above the class's maxDocuments
// are rejected rather than truncated.
func (c *client) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	settings := config.NewClassSettings(cfg)

	if query == "" {
		return nil, errors.New("no query provided")
	}
	// The limits are applied here as well because a class that never names
	// this module in its moduleConfig is not validated on creation.
	if maxDocuments := min(settings.MaxDocuments(), config.MaxDocumentsLimit); len(documents) > maxDocuments {
		return nil, errors.Errorf("%d documents exceed maxDocuments %d", len(documents), maxDocuments)
	}
	batchSize := min(max(settings.BatchSize(), 1), config.MaxBatchSize)
	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Jev API Key")
	}
	jevURL, err := c.getJevURL(ctx, settings.BaseURL())
	if err != nil {
		return nil, err
	}
	levels, err := config.ScoreLevels(ctx, cfg)
	if err != nil {
		return nil, err
	}
	cache, err := cacheModeOf(ctx)
	if err != nil {
		return nil, err
	}
	request := judgeRequest{
		url: jevURL, apiKey: apiKey, model: settings.Model(), query: query, batchSize: batchSize,
		levels: levels,
	}

	// Documents with the same content share one judgment. firstIndex maps a
	// document to the position that holds its score.
	firstIndex := make(map[string]int, len(documents))
	scores := make([]float64, len(documents))
	// keys holds the cache keys of every document that has to be judged.
	keys := make(map[int]documentKeys)
	var pending []int
	for i, document := range documents {
		if document == "" {
			continue
		}
		if _, ok := firstIndex[document]; ok {
			continue
		}
		firstIndex[document] = i
		documentKeys := request.cacheKeys(document)
		if cache == cacheOn {
			if score, ok := c.cache.get(documentKeys.batched); ok {
				scores[i] = score
				continue
			}
			// A judgment made alone is the stable one, so every batch size
			// may use it.
			if documentKeys.batched != documentKeys.single {
				if score, ok := c.cache.get(documentKeys.single); ok {
					scores[i] = score
					continue
				}
			}
		}
		keys[i] = documentKeys
		pending = append(pending, i)
	}

	// Every text empty means the rerank property is not a text property of
	// these objects, most likely a typo. Judging nothing would look like
	// "nothing matched".
	if len(documents) > 0 && len(firstIndex) == 0 {
		return nil, errors.New("no document has text: check the rerank property")
	}

	var inputTokens, requests atomic.Int64
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(cap(c.inFlight))
	for start := 0; start < len(pending); start += batchSize {
		batch := pending[start:min(start+batchSize, len(pending))]
		eg.Go(func() error {
			batchDocuments := make([]string, len(batch))
			for j, index := range batch {
				batchDocuments[j] = documents[index]
			}
			probabilities, usage, err := c.judge(ctx, request, batchDocuments)
			if err != nil {
				return err
			}
			for j, index := range batch {
				// A document that was sent alone is a single judgment even
				// when the class batches: the last batch can hold one
				// document.
				key := keys[index].batched
				if len(batch) == 1 {
					key = keys[index].single
				}
				switch cache {
				case cacheOff:
					scores[index] = probabilities[j]
				case cacheRefresh:
					// A batched class reads the batched key first, so a stale
					// answer must not survive under either key.
					c.cache.put(key, probabilities[j])
					if other := keys[index].batched; other != key {
						c.cache.remove(other)
					}
					scores[index] = probabilities[j]
				default:
					// Another query may have judged the document meanwhile.
					// The answer stored first is the one every query uses.
					scores[index] = c.cache.putIfAbsent(key, probabilities[j])
				}
			}
			inputTokens.Add(usage.InputTokens)
			requests.Add(1)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	c.logger.WithField("action", "reranker_jev_rank").
		WithField("documents", len(documents)).
		WithField("judged", len(pending)).
		WithField("requests", requests.Load()).
		WithField("input_tokens", inputTokens.Load()).
		Debug("jev rank finished")

	documentScores := make([]ent.DocumentScore, len(documents))
	for i, document := range documents {
		score := 0.0
		if owner, ok := firstIndex[document]; ok {
			score = scores[owner]
		}
		documentScores[i] = ent.DocumentScore{Document: document, Score: score}
	}
	return &ent.RankResult{Query: query, DocumentScores: documentScores}, nil
}

// The cache modes a request can ask for with the X-Jev-Cache header.
const (
	cacheHeader = "X-Jev-Cache"
	// cacheOn reads stored answers and stores new ones. It is the default.
	cacheOn = "on"
	// cacheRefresh judges every document again and replaces what is stored.
	cacheRefresh = "refresh"
	// cacheOff judges every document again and stores nothing.
	cacheOff = "off"
)

func cacheModeOf(ctx context.Context) (string, error) {
	switch mode := modulecomponents.GetValueFromContext(ctx, cacheHeader); mode {
	case "":
		return cacheOn, nil
	case cacheOn, cacheRefresh, cacheOff:
		return mode, nil
	default:
		return "", errors.Errorf("%s must be %q, %q or %q, got %q", cacheHeader, cacheOn, cacheRefresh, cacheOff, mode)
	}
}

// judgeRequest is what all batches of one Rank call have in common.
type judgeRequest struct {
	url       string
	apiKey    string
	model     string
	query     string
	batchSize int
	// levels is the rubric of a score question. Empty for a yes/no question.
	levels []string
}

// documentKeys are the cache keys of one document: for a judgment made in a
// batch of the class's batch size, and for one made alone. They are equal
// when the class does not batch.
type documentKeys struct {
	batched judgmentKey
	single  judgmentKey
}

// cacheKeys covers everything that can change the answer or who may read it.
// Another endpoint or API key must not see judgments made for this one, a
// probability judged in a batch differs from one judged alone, and the
// rubric decides what the number means.
func (r judgeRequest) cacheKeys(document string) documentKeys {
	single := r.cacheKey(document, 1)
	if r.batchSize == 1 {
		return documentKeys{batched: single, single: single}
	}
	return documentKeys{batched: r.cacheKey(document, r.batchSize), single: single}
}

func (r judgeRequest) cacheKey(document string, batchSize int) judgmentKey {
	parts := make([]string, 0, 7+len(r.levels))
	parts = append(parts, r.url, r.apiKey, r.model, strconv.Itoa(batchSize), strconv.Itoa(len(r.levels)))
	parts = append(parts, r.levels...)
	parts = append(parts, r.query, document)
	return newJudgmentKey(parts...)
}

// judge sends one batch and retries when the API is rate limiting or failing.
// A batch the API rejects for its size is sent as two halves, so long
// documents still get judged; the batch size is an upper bound. It returns
// one probability per document, in order.
func (c *client) judge(ctx context.Context, request judgeRequest, documents []string,
) ([]float64, jevUsage, error) {
	values, usage, result, err := c.judgeBatch(ctx, request, documents)
	if err == nil || !result.tooLarge || len(documents) < 2 {
		return values, usage, err
	}
	half := len(documents) / 2
	first, firstUsage, err := c.judge(ctx, request, documents[:half])
	if err != nil {
		return nil, jevUsage{}, err
	}
	second, secondUsage, err := c.judge(ctx, request, documents[half:])
	if err != nil {
		return nil, jevUsage{}, err
	}
	return append(first, second...), jevUsage{InputTokens: firstUsage.InputTokens + secondUsage.InputTokens}, nil
}

func (c *client) judgeBatch(ctx context.Context, request judgeRequest, documents []string,
) ([]float64, jevUsage, sendResult, error) {
	body, err := json.Marshal(newJevRequest(request, documents))
	if err != nil {
		return nil, jevUsage{}, sendResult{}, errors.Wrap(err, "marshal body")
	}

	var lastErr error
	retryAfter := time.Duration(0)
	for attempt := range maxAttempts {
		if attempt > 0 {
			if err := wait(ctx, c.retryDelay(attempt, retryAfter)); err != nil {
				return nil, jevUsage{}, sendResult{}, err
			}
		}
		result, err := c.send(ctx, request, body, len(documents))
		if err == nil {
			return result.values, result.usage, result, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, jevUsage{}, result, ctxErr
		}
		if !result.retryable {
			return nil, jevUsage{}, result, err
		}
		c.logger.WithField("action", "reranker_jev_retry").WithField("attempt", attempt+1).
			Debugf("jev request failed, retrying: %v", err)
		lastErr = err
		retryAfter = result.retryAfter
	}
	return nil, jevUsage{}, sendResult{}, errors.Wrapf(lastErr, "after %d attempts", maxAttempts)
}

// retryDelay doubles with every attempt and is spread over half its length,
// so that batches that failed together do not retry at the same instant. A
// Retry-After from the API wins when it asks for more.
func (c *client) retryDelay(attempt int, retryAfter time.Duration) time.Duration {
	backoff := c.retryBackoff << (attempt - 1)
	delay := backoff/2 + rand.N(backoff/2+1)
	return max(delay, min(retryAfter, maxRetryAfter))
}

type sendResult struct {
	values     []float64
	usage      jevUsage
	retryable  bool
	retryAfter time.Duration
	// tooLarge: the API rejected the request for its size.
	tooLarge bool
}

// tooLargeError is the error_type Jev answers with status 400 when a
// request has more tokens than it accepts. The limit is not published.
const tooLargeError = "max_tokens_exceeded"

func (c *client) send(ctx context.Context, request judgeRequest, body []byte, documents int,
) (sendResult, error) {
	select {
	case c.inFlight <- struct{}{}:
		defer func() { <-c.inFlight }()
	case <-ctx.Done():
		return sendResult{}, ctx.Err()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, request.url, bytes.NewReader(body))
	if err != nil {
		return sendResult{}, errors.Wrap(err, "create POST request")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", request.apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if err != nil {
		// Not retried: the base client already resends on a broken
		// connection, and retrying a timeout multiplies the wait.
		return sendResult{}, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		errorBody, _ := io.ReadAll(io.LimitReader(res.Body, maxErrorBodyBytes))
		return sendResult{
				retryable:  res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500,
				retryAfter: parseRetryAfter(res.Header.Get("Retry-After")),
				tooLarge:   res.StatusCode == http.StatusBadRequest && bytes.Contains(errorBody, []byte(tooLargeError)),
			}, errors.Errorf(
				"connection to Jev API failed with status %d: %s", res.StatusCode, errorBody)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return sendResult{}, errors.Wrap(err, "read response body")
	}
	var response jevResponse
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return sendResult{}, errors.Wrap(err, "parse response")
	}
	values, err := response.values(documents, len(request.levels))
	if err != nil {
		return sendResult{}, err
	}
	return sendResult{values: values, usage: response.Usage}, nil
}

// parseRetryAfter reads the seconds form of the Retry-After header. Anything
// else gives 0.
func parseRetryAfter(header string) time.Duration {
	seconds, err := strconv.Atoi(header)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *client) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Jev-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Jev-Api-Key " +
		"nor in environment variable under JEV_APIKEY")
}

func (c *client) getJevURL(ctx context.Context, baseURL string) (string, error) {
	passedBaseURL, err := modulecomponents.ValidatedBaseURLFromHeader(ctx, "X-Jev-Baseurl", baseURL)
	if err != nil {
		return "", err
	}
	return url.JoinPath(passedBaseURL, apiPath)
}

type jevRequest struct {
	Model     string                 `json:"model"`
	State     any                    `json:"state"`
	Questions map[string]jevQuestion `json:"questions"`
}

type jevQuestion struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions"`
	// Criteria is the rubric of a score question, lowest level first.
	Criteria []string `json:"criteria,omitempty"`
}

// newJevRequest builds the request for one batch. A single document is the
// state itself. Several documents become a map, with one question per
// document that names the document it is about.
func newJevRequest(request judgeRequest, documents []string) jevRequest {
	question := jevQuestion{Type: noulType, Instructions: request.query}
	if len(request.levels) > 0 {
		question.Type = scoreType
		question.Criteria = request.levels
	}
	if len(documents) == 1 {
		return jevRequest{
			Model:     request.model,
			State:     documents[0],
			Questions: map[string]jevQuestion{questionKey: question},
		}
	}
	state := make(map[string]string, len(documents))
	questions := make(map[string]jevQuestion, len(documents))
	for i, document := range documents {
		key := batchKey(i)
		state[key] = document
		batchQuestion := question
		batchQuestion.Instructions = "In " + key + ": " + request.query
		questions[key] = batchQuestion
	}
	return jevRequest{Model: request.model, State: state, Questions: questions}
}

func batchKey(i int) string {
	return "document_" + strconv.Itoa(i)
}

type jevResponse struct {
	Answers map[string]jevAnswer `json:"answers"`
	Usage   jevUsage             `json:"usage"`
}

type jevAnswer struct {
	Type  string   `json:"type"`
	Noul  *float64 `json:"noul,omitempty"`
	Score *float64 `json:"score,omitempty"`
}

// jevUsage is the part of the API's usage report that is billed.
type jevUsage struct {
	InputTokens int64 `json:"input_tokens"`
}

// values returns the answers of a batch of the given size, in the order of
// its documents. levels is the length of the rubric, 0 for a yes/no question.
func (r jevResponse) values(documents, levels int) ([]float64, error) {
	if documents == 1 {
		value, err := r.value(questionKey, levels)
		if err != nil {
			return nil, err
		}
		return []float64{value}, nil
	}
	out := make([]float64, documents)
	for i := range out {
		value, err := r.value(batchKey(i), levels)
		if err != nil {
			return nil, err
		}
		out[i] = value
	}
	return out, nil
}

func (r jevResponse) value(key string, levels int) (float64, error) {
	answer, ok := r.Answers[key]
	if !ok {
		return 0, errors.Errorf("no answer in response for %q", key)
	}
	if levels > 0 {
		if answer.Type != scoreType {
			return 0, errors.Errorf("unexpected answer type %q", answer.Type)
		}
		if answer.Score == nil {
			return 0, errors.New("no score in response")
		}
		// A score is a position on the rubric, 0 for the first level.
		if s := *answer.Score; s < 0 || s > float64(levels-1) {
			return 0, errors.Errorf("score %v out of range", s)
		}
		return *answer.Score, nil
	}
	if answer.Type != noulType {
		return 0, errors.Errorf("unexpected answer type %q", answer.Type)
	}
	if answer.Noul == nil {
		return 0, errors.New("no probability in response")
	}
	if p := *answer.Noul; p < 0 || p > 1 {
		return 0, errors.Errorf("probability %v out of range", p)
	}
	return *answer.Noul, nil
}
