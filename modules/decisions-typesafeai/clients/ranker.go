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
	"github.com/weaviate/weaviate/modules/decisions-typesafeai/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const (
	apiPath     = "/v1/systemone"
	questionKey = "q"
	noulType    = "noul"
	scoreType   = "score"

	// DefaultMaxConcurrentRequests and MaxConcurrentRequestsLimit bound the
	// requests to the TypeSafeAI API that are in flight across all queries of the
	// process.
	DefaultMaxConcurrentRequests = 16
	MaxConcurrentRequestsLimit   = 256

	maxAttempts = 4
	// maxRetryAfter caps how long a Retry-After header can delay a retry.
	maxRetryAfter = 10 * time.Second
	// maxErrorBodyBytes bounds how much of an error response ends up in the
	// error message.
	maxErrorBodyBytes = 512
	// maxResponseBodyBytes bounds how much of a 200 response is read. A
	// response holds one short answer per document, so a body this large is
	// not from the API.
	maxResponseBodyBytes = 1 << 20

	// MaxBatchDocuments and MaxBatchCharacters bound one request to the
	// API, whichever is reached first. The API judges one state per request
	// and the documents of a batch share it, so a request costs its fixed
	// tokens once for all of them. The size the API accepts is not
	// published: 141,000 characters of documents were accepted and 149,000
	// rejected, so 100,000 leaves room for the questions. Beyond 50
	// documents a request saves almost no tokens and answers more slowly
	// than two sent in parallel.
	MaxBatchDocuments  = 50
	MaxBatchCharacters = 100_000
)

type client struct {
	apiKey       string
	httpClient   *http.Client
	retryBackoff time.Duration
	// inFlight holds one token per request being sent. It is shared by all
	// Rank calls, so it limits the load on the TypeSafeAI API however many queries
	// run at once.
	inFlight chan struct{}
	logger   logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, maxConcurrentRequests int, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:       apiKey,
		httpClient:   modulecomponents.NewBaseHttpClient(timeout),
		retryBackoff: 500 * time.Millisecond,
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
// Documents are sent in batches, see MaxBatchDocuments. The probability of a
// document then depends a little on the documents it shares the request
// with. Identical documents are judged once. Requests above the class's
// maxDocuments are rejected rather than truncated.
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
	typesafeaiURL, apiKey, err := c.endpoint(ctx, settings.BaseURL())
	if err != nil {
		return nil, err
	}
	levels, err := config.ScoreLevels(ctx, cfg)
	if err != nil {
		return nil, err
	}
	request := judgeRequest{url: typesafeaiURL, apiKey: apiKey, model: settings.Model(), query: query, levels: levels}

	// Documents with the same content share one judgment. firstIndex maps a
	// document to the position that holds its score.
	firstIndex := make(map[string]int, len(documents))
	scores := make([]float64, len(documents))
	var pending []int
	for i, document := range documents {
		if document == "" {
			continue
		}
		if _, ok := firstIndex[document]; ok {
			continue
		}
		firstIndex[document] = i
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
	for _, batch := range batches(pending, documents) {
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
				scores[index] = probabilities[j]
			}
			inputTokens.Add(usage.InputTokens)
			requests.Add(1)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	c.logger.WithField("action", "decisions_typesafeai_rank").
		WithField("documents", len(documents)).
		WithField("judged", len(pending)).
		WithField("requests", requests.Load()).
		WithField("input_tokens", inputTokens.Load()).
		Debug("typesafeai rank finished")

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

// batches splits the positions of the documents to judge into requests of
// at most MaxBatchDocuments documents and MaxBatchCharacters characters,
// in order. A document longer than that goes alone; judge halves a request
// the API still rejects.
func batches(indexes []int, documents []string) [][]int {
	var out [][]int
	var batch []int
	characters := 0
	for _, index := range indexes {
		length := len(documents[index])
		if len(batch) > 0 && (len(batch) == MaxBatchDocuments || characters+length > MaxBatchCharacters) {
			out = append(out, batch)
			batch, characters = nil, 0
		}
		batch = append(batch, index)
		characters += length
	}
	if len(batch) > 0 {
		out = append(out, batch)
	}
	return out
}

// judgeRequest is what all batches of one Rank call have in common.
type judgeRequest struct {
	url    string
	apiKey string
	model  string
	query  string
	// levels is the rubric of a score question. Empty for a yes/no question.
	levels []string
}

// judge sends one batch and retries when the API is rate limiting or failing.
// A batch the API rejects for its size is sent as two halves, so long
// documents still get judged; the batch size is an upper bound. It returns
// one probability per document, in order.
func (c *client) judge(ctx context.Context, request judgeRequest, documents []string,
) ([]float64, typesafeaiUsage, error) {
	values, usage, result, err := c.judgeBatch(ctx, request, documents)
	if err == nil || !result.tooLarge || len(documents) < 2 {
		return values, usage, err
	}
	half := len(documents) / 2
	first, firstUsage, err := c.judge(ctx, request, documents[:half])
	if err != nil {
		return nil, typesafeaiUsage{}, err
	}
	second, secondUsage, err := c.judge(ctx, request, documents[half:])
	if err != nil {
		return nil, typesafeaiUsage{}, err
	}
	return append(first, second...), typesafeaiUsage{InputTokens: firstUsage.InputTokens + secondUsage.InputTokens}, nil
}

func (c *client) judgeBatch(ctx context.Context, request judgeRequest, documents []string,
) ([]float64, typesafeaiUsage, sendResult, error) {
	body, err := json.Marshal(newTypeSafeAIRequest(request, documents))
	if err != nil {
		return nil, typesafeaiUsage{}, sendResult{}, errors.Wrap(err, "marshal body")
	}

	response, result, err := c.postWithRetry(ctx, request.url, request.apiKey, body)
	if err != nil {
		return nil, typesafeaiUsage{}, result, err
	}
	values, err := response.values(len(documents), len(request.levels))
	if err != nil {
		return nil, typesafeaiUsage{}, sendResult{}, err
	}
	return values, response.Usage, result, nil
}

// postWithRetry sends the body and retries when the API is rate limiting or
// failing. The result says whether the API rejected the request for its
// size.
func (c *client) postWithRetry(ctx context.Context, url, apiKey string, body []byte,
) (typesafeaiResponse, sendResult, error) {
	var lastErr error
	retryAfter := time.Duration(0)
	for attempt := range maxAttempts {
		if attempt > 0 {
			if err := wait(ctx, c.retryDelay(attempt, retryAfter)); err != nil {
				return typesafeaiResponse{}, sendResult{}, err
			}
		}
		response, result, err := c.send(ctx, url, apiKey, body)
		if err == nil {
			return response, result, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return typesafeaiResponse{}, result, ctxErr
		}
		if !result.retryable {
			return typesafeaiResponse{}, result, err
		}
		c.logger.WithField("action", "decisions_typesafeai_retry").WithField("attempt", attempt+1).
			Debugf("typesafeai request failed, retrying: %v", err)
		lastErr = err
		retryAfter = result.retryAfter
	}
	return typesafeaiResponse{}, sendResult{}, errors.Wrapf(lastErr, "after %d attempts", maxAttempts)
}

// retryDelay doubles with every attempt and is spread over half its length,
// so that batches that failed together do not retry at the same instant. A
// Retry-After from the API wins when it asks for more.
func (c *client) retryDelay(attempt int, retryAfter time.Duration) time.Duration {
	backoff := c.retryBackoff << (attempt - 1)
	delay := backoff/2 + rand.N(backoff/2+1)
	return max(delay, min(retryAfter, maxRetryAfter))
}

// sendResult says how a failed request may be handled.
type sendResult struct {
	retryable  bool
	retryAfter time.Duration
	// tooLarge: the API rejected the request for its size.
	tooLarge bool
}

// tooLargeError is the error_type Jev answers with status 400 when a
// request has more tokens than it accepts. The limit is not published.
const tooLargeError = "max_tokens_exceeded"

func (c *client) send(ctx context.Context, url, apiKey string, body []byte,
) (typesafeaiResponse, sendResult, error) {
	select {
	case c.inFlight <- struct{}{}:
		defer func() { <-c.inFlight }()
	case <-ctx.Done():
		return typesafeaiResponse{}, sendResult{}, ctx.Err()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return typesafeaiResponse{}, sendResult{}, errors.Wrap(err, "create POST request")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if err != nil {
		// Not retried: the base client already resends on a broken
		// connection, and retrying a timeout multiplies the wait.
		return typesafeaiResponse{}, sendResult{}, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		errorBody, _ := io.ReadAll(io.LimitReader(res.Body, maxErrorBodyBytes))
		return typesafeaiResponse{}, sendResult{
				retryable:  res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500,
				retryAfter: parseRetryAfter(res.Header.Get("Retry-After")),
				tooLarge:   res.StatusCode == http.StatusBadRequest && bytes.Contains(errorBody, []byte(tooLargeError)),
			}, errors.Errorf(
				"connection to TypeSafeAI API failed with status %d: %s", res.StatusCode, errorBody)
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBodyBytes))
	if err != nil {
		return typesafeaiResponse{}, sendResult{}, errors.Wrap(err, "read response body")
	}
	if len(bodyBytes) >= maxResponseBodyBytes {
		return typesafeaiResponse{}, sendResult{}, errors.Errorf("response body reaches the limit of %d bytes", maxResponseBodyBytes)
	}
	var response typesafeaiResponse
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return typesafeaiResponse{}, sendResult{}, errors.Wrap(err, "parse response")
	}
	return response, sendResult{}, nil
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

const (
	apiKeyHeader  = "X-Typesafeai-Api-Key"
	baseURLHeader = "X-Typesafeai-Baseurl"
)

// endpoint returns the URL and the API key of the requests. A base URL from
// the request header is only used with an API key from the request header,
// so the server's key goes to the class's baseURL or the default and nowhere
// else.
func (c *client) endpoint(ctx context.Context, classBaseURL string) (endpointURL, apiKey string, err error) {
	apiKey = modulecomponents.GetValueFromContext(ctx, apiKeyHeader)
	baseURL := classBaseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, baseURLHeader); headerBaseURL != "" {
		if apiKey == "" {
			return "", "", errors.Errorf("%s needs %s: the server's key is not sent to a host named by the request",
				baseURLHeader, apiKeyHeader)
		}
		if err := modulecomponents.ValidateBaseURL(headerBaseURL); err != nil {
			return "", "", err
		}
		baseURL = headerBaseURL
	}
	if apiKey == "" {
		apiKey = c.apiKey
	}
	if apiKey == "" {
		return "", "", errors.Errorf("TypeSafeAI API Key: no api key found "+
			"neither in request header: %s "+
			"nor in environment variable under TYPESAFEAI_APIKEY", apiKeyHeader)
	}
	endpointURL, err = url.JoinPath(baseURL, apiPath)
	return endpointURL, apiKey, err
}

type typesafeaiRequest struct {
	Model     string                        `json:"model"`
	State     any                           `json:"state"`
	Questions map[string]typesafeaiQuestion `json:"questions"`
}

type typesafeaiQuestion struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions"`
	// Criteria is the rubric of a score question, lowest level first, or
	// the options of a choice question with their descriptions.
	Criteria any `json:"criteria,omitempty"`
}

// newTypeSafeAIRequest builds the request for one batch. A single document is the
// state itself. Several documents become a map, with one question per
// document that names the document it is about.
func newTypeSafeAIRequest(request judgeRequest, documents []string) typesafeaiRequest {
	question := typesafeaiQuestion{Type: noulType, Instructions: request.query}
	if len(request.levels) > 0 {
		question.Type = scoreType
		question.Criteria = request.levels
	}
	if len(documents) == 1 {
		return typesafeaiRequest{
			Model:     request.model,
			State:     documents[0],
			Questions: map[string]typesafeaiQuestion{questionKey: question},
		}
	}
	state := make(map[string]string, len(documents))
	questions := make(map[string]typesafeaiQuestion, len(documents))
	for i, document := range documents {
		key := batchKey(i)
		state[key] = document
		batchQuestion := question
		batchQuestion.Instructions = "In " + key + ": " + request.query
		questions[key] = batchQuestion
	}
	return typesafeaiRequest{Model: request.model, State: state, Questions: questions}
}

func batchKey(i int) string {
	return "document_" + strconv.Itoa(i)
}

type typesafeaiResponse struct {
	Answers map[string]typesafeaiAnswer `json:"answers"`
	Usage   typesafeaiUsage             `json:"usage"`
}

type typesafeaiAnswer struct {
	Type  string   `json:"type"`
	Noul  *float64 `json:"noul,omitempty"`
	Score *float64 `json:"score,omitempty"`
	// Choice, Probabilities and Confidence are the parts of a choice or a
	// score answer. A score's probabilities are keyed by level index.
	Choice        *string            `json:"choice,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Confidence    *float64           `json:"confidence,omitempty"`
}

// typesafeaiUsage is the part of the API's usage report that is billed.
type typesafeaiUsage struct {
	InputTokens int64 `json:"input_tokens"`
}

// values returns the answers of a batch of the given size, in the order of
// its documents. levels is the length of the rubric, 0 for a yes/no question.
func (r typesafeaiResponse) values(documents, levels int) ([]float64, error) {
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

func (r typesafeaiResponse) value(key string, levels int) (float64, error) {
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
