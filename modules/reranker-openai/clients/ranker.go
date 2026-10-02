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
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-openai/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// The predicate question asked about every document. The wording of the
// relevance question is generic on purpose: it is not tuned to a dataset or
// a kind of query. With the statement question the rerank query is the
// instruction itself.
const (
	relevanceInstructions = "Does the document answer or directly address this query? Query: %s"

	// The question names, echoed by the API in its answer.
	relevanceName = "relevance"
	statementName = "statement"

	predicateType = "predicate"
	refusalType   = "refusal"
)

const (
	apiPath = "/v1/decisions"

	// DefaultMaxConcurrentRequests and MaxConcurrentRequestsLimit bound the
	// requests to the OpenAI API that are in flight across all queries of
	// the process.
	DefaultMaxConcurrentRequests = 32
	MaxConcurrentRequestsLimit   = 256

	maxAttempts = 4
	// maxRetryAfter caps how long a Retry-After header can delay a retry.
	maxRetryAfter = 10 * time.Second
	// maxErrorBodyBytes bounds how much of an error response is read, and
	// maxErrorMessageBytes how much of its message ends up in the error.
	maxErrorBodyBytes    = 16 * 1024
	maxErrorMessageBytes = 512
	// maxResponseBodyBytes bounds how much of a 200 response is read. A
	// decision with one answer is a few hundred bytes.
	maxResponseBodyBytes = 1 << 20

	apiKeyHeader  = "X-Openai-Api-Key"
	baseURLHeader = "X-Openai-Baseurl"

	insufficientQuota = "insufficient_quota"
)

type client struct {
	apiKey       string
	httpClient   *http.Client
	retryBackoff time.Duration
	// inFlight holds one token per request being sent. It is shared by all
	// Rank calls, so it limits the load on the OpenAI API however many
	// queries run at once.
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

// Rank asks the Decisions API, once per document, whether the document
// answers the query. The score is the probability the API returns for that
// predicate. A document the model refuses to judge scores 0.
//
// Identical documents share one request and empty documents are not sent.
// Requests above the class's maxDocuments are rejected rather than truncated.
func (c *client) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	// The settings are checked here as well because a class that never names
	// this module in its moduleConfig is not validated on creation.
	settings, err := config.NewClassSettings(cfg).Resolve()
	if err != nil {
		return nil, err
	}
	if query == "" {
		return nil, errors.New("no query provided")
	}
	if maxDocuments := min(settings.MaxDocuments, config.MaxDocumentsLimit); len(documents) > maxDocuments {
		return nil, errors.Errorf("%d documents exceed maxDocuments %d", len(documents), maxDocuments)
	}
	openAIURL, apiKey, err := c.endpoint(ctx, settings.BaseURL)
	if err != nil {
		return nil, err
	}
	request := judgeRequest{
		url: openAIURL, apiKey: apiKey, model: settings.Model, query: query,
		statement: settings.Question == config.QuestionStatement,
	}

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
	// these objects, most likely a typo. Scoring nothing would look like
	// "nothing matched".
	if len(documents) > 0 && len(firstIndex) == 0 {
		return nil, errors.New("no document has text: check the rerank property")
	}

	var inputTokens, outputTokens, requests, unanswered atomic.Int64
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(cap(c.inFlight))
	for _, index := range pending {
		eg.Go(func() error {
			judgment, err := c.judge(ctx, request, documents[index])
			if err != nil {
				return err
			}
			scores[index] = judgment.score
			if !judgment.answered {
				unanswered.Add(1)
			}
			inputTokens.Add(judgment.usage.InputTokens)
			outputTokens.Add(judgment.usage.OutputTokens)
			requests.Add(1)
			return nil
		})
	}
	err = eg.Wait()
	usage := c.logger.WithField("action", "reranker_openai_rank").
		WithField("documents", len(documents)).
		WithField("requests", requests.Load()).
		WithField("unanswered", unanswered.Load()).
		WithField("input_tokens", inputTokens.Load()).
		WithField("output_tokens", outputTokens.Load())
	if err != nil {
		// The answered documents were billed although the query failed.
		if requests.Load() > 0 {
			usage.Debug("openai rerank failed")
		}
		return nil, err
	}
	usage.Debug("openai rerank finished")

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

// judgeRequest is what all requests of one Rank call have in common.
type judgeRequest struct {
	url    string
	apiKey string
	model  string
	query  string
	// statement selects the statement question instead of the relevance one.
	statement bool
}

// questionName is the name the request gives its question and the answer
// must carry.
func (r judgeRequest) questionName() string {
	if r.statement {
		return statementName
	}
	return relevanceName
}

type judgment struct {
	score float64
	// answered is false when the model refused to judge the document. The
	// score is then 0.
	answered bool
	usage    decisionUsage
}

// judge sends one document and retries when the API is rate limiting or
// failing.
func (c *client) judge(ctx context.Context, request judgeRequest, document string) (judgment, error) {
	body, err := json.Marshal(newDecisionRequest(request, document))
	if err != nil {
		return judgment{}, errors.Wrap(err, "marshal body")
	}

	var lastErr error
	retryAfter := time.Duration(0)
	for attempt := range maxAttempts {
		if attempt > 0 {
			if err := wait(ctx, c.retryDelay(attempt, retryAfter)); err != nil {
				return judgment{}, err
			}
		}
		result, err := c.send(ctx, request, body)
		if err == nil {
			return result.judgment, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return judgment{}, ctxErr
		}
		if !result.retryable {
			return judgment{}, err
		}
		c.logger.WithField("action", "reranker_openai_retry").WithField("attempt", attempt+1).
			Debugf("openai request failed, retrying: %v", err)
		lastErr = err
		retryAfter = result.retryAfter
	}
	return judgment{}, errors.Wrapf(lastErr, "after %d attempts", maxAttempts)
}

// retryDelay doubles with every attempt and is spread over half its length,
// so that requests that failed together do not retry at the same instant. A
// Retry-After from the API wins when it asks for more.
func (c *client) retryDelay(attempt int, retryAfter time.Duration) time.Duration {
	backoff := c.retryBackoff << (attempt - 1)
	delay := backoff/2 + rand.N(backoff/2+1)
	return max(delay, min(retryAfter, maxRetryAfter))
}

type sendResult struct {
	judgment   judgment
	retryable  bool
	retryAfter time.Duration
}

func (c *client) send(ctx context.Context, request judgeRequest, body []byte) (sendResult, error) {
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
		apiErr := readAPIError(res.Body)
		return sendResult{
			retryable:  retryable(res.StatusCode, apiErr),
			retryAfter: parseRetryAfter(res.Header.Get("Retry-After")),
		}, statusError(res.StatusCode, apiErr)
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBodyBytes))
	if err != nil {
		return sendResult{}, errors.Wrap(err, "read response body")
	}
	if len(bodyBytes) >= maxResponseBodyBytes {
		return sendResult{}, errors.Errorf("response body reaches the limit of %d bytes", maxResponseBodyBytes)
	}
	var response decisionResponse
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return sendResult{}, errors.Wrap(err, "parse response")
	}
	if response.Error != nil {
		if message := response.Error.message(); message != "" {
			return sendResult{}, errors.Errorf("OpenAI API returned an error: %s", message)
		}
		return sendResult{}, errors.New("OpenAI API returned an error without a message")
	}
	score, answered, err := response.score(request.questionName())
	if err != nil {
		return sendResult{}, err
	}
	return sendResult{judgment: judgment{score: score, answered: answered, usage: response.Usage}}, nil
}

// apiError is the "error" object of an OpenAI response.
type apiError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	// Code is a string or null from OpenAI. Compatible servers also send a
	// number.
	Code any `json:"code"`
}

// message is cut to maxErrorMessageBytes.
func (e apiError) message() string {
	if len(e.Message) > maxErrorMessageBytes {
		return strings.ToValidUTF8(e.Message[:maxErrorMessageBytes], "")
	}
	return e.Message
}

// readAPIError returns the zero value when the body is not OpenAI's error
// format.
func readAPIError(body io.Reader) apiError {
	var parsed struct {
		Error apiError `json:"error"`
	}
	errorBody, _ := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes))
	if err := json.Unmarshal(errorBody, &parsed); err != nil {
		return apiError{}
	}
	return parsed.Error
}

// retryable is true for a rate limit and a server error. A 429 for an
// exhausted quota is not a rate limit: it fails until the account is paid.
func retryable(status int, apiErr apiError) bool {
	if status == http.StatusTooManyRequests {
		return apiErr.Code != insufficientQuota && apiErr.Type != insufficientQuota
	}
	return status >= 500
}

// statusError names the status and, when the body has one, OpenAI's error
// message. The rest of the body is left out.
func statusError(status int, apiErr apiError) error {
	// OpenAI's message for a rejected key contains a masked form of the key.
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return errors.Errorf("OpenAI rejected the API key (status %d)", status)
	}
	message := apiErr.message()
	if message == "" {
		return errors.Errorf("connection to OpenAI API failed with status %d", status)
	}
	return errors.Errorf("connection to OpenAI API failed with status %d: %s", status, message)
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

// endpoint returns the URL and the API key of the requests. A base URL from
// the request header is only used with an API key from the request header, so
// the server's key goes to the class's baseURL or the default and nowhere else.
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
		if baseURL, err = config.NormalizeBaseURL(baseURLHeader, headerBaseURL); err != nil {
			return "", "", err
		}
	}
	if apiKey == "" {
		apiKey = c.apiKey
	}
	if apiKey == "" {
		return "", "", errors.Errorf("OpenAI API Key: no api key found "+
			"neither in request header: %s "+
			"nor in environment variable under OPENAI_APIKEY", apiKeyHeader)
	}
	endpointURL, err = url.JoinPath(baseURL, apiPath)
	return endpointURL, apiKey, err
}

// decisionRequest is the body of POST /v1/decisions: the document as the
// input and one predicate question about it.
type decisionRequest struct {
	Model     string             `json:"model"`
	Input     string             `json:"input"`
	Questions []decisionQuestion `json:"questions"`
}

type decisionQuestion struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Instructions string `json:"instructions"`
}

func newDecisionRequest(request judgeRequest, document string) decisionRequest {
	instructions := fmt.Sprintf(relevanceInstructions, request.query)
	if request.statement {
		instructions = request.query
	}
	return decisionRequest{
		Model: request.model,
		Input: document,
		Questions: []decisionQuestion{
			{Type: predicateType, Name: request.questionName(), Instructions: instructions},
		},
	}
}

type decisionResponse struct {
	Answers []decisionAnswer `json:"answers"`
	Usage   decisionUsage    `json:"usage"`
	Error   *apiError        `json:"error"`
}

// decisionAnswer is a predicate answer or a refusal. The fields of the
// other answer types are not read.
type decisionAnswer struct {
	Type string  `json:"type"`
	Name *string `json:"name"`
	// Probability is a pointer because a missing value must not read as 0.
	Probability *float64 `json:"probability"`
}

// decisionUsage is the part of the API's usage report that is billed. The
// Decisions API reports no output tokens today; the field is kept so that a
// change shows in the log.
type decisionUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// score returns the probability of the predicate named name. answered is
// false, and the score 0, when the model refused to answer.
func (r decisionResponse) score(name string) (score float64, answered bool, err error) {
	if len(r.Answers) != 1 {
		return 0, false, errors.Errorf("expected one answer in response, got %d", len(r.Answers))
	}
	answer := r.Answers[0]
	if answer.Name == nil || *answer.Name != name {
		got := "no name"
		if answer.Name != nil {
			got = strconv.Quote(*answer.Name)
		}
		return 0, false, errors.Errorf("answer in response is for %s, not for %q", got, name)
	}
	switch answer.Type {
	case refusalType:
		return 0, false, nil
	case predicateType:
		if answer.Probability == nil {
			return 0, false, errors.New("no probability in response")
		}
		if p := *answer.Probability; p < 0 || p > 1 {
			return 0, false, errors.Errorf("probability %v in response is outside [0, 1]", p)
		}
		return *answer.Probability, true, nil
	default:
		return 0, false, errors.Errorf("unexpected answer type %q in response", answer.Type)
	}
}
