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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/reranker-openai/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const (
	DefaultMaxConcurrentRequests = 32
	MaxConcurrentRequestsLimit   = 256

	apiPath              = "/v1/decisions"
	predicateType        = "predicate"
	refusalType          = "refusal"
	questionName         = "condition"
	apiKeyHeader         = "X-Openai-Api-Key"
	baseURLHeader        = "X-Openai-Baseurl"
	organizationHeader   = "X-Openai-Organization"
	maxErrorBodyBytes    = 16 * 1024
	maxErrorMessageBytes = 512
	maxResponseBodyBytes = 1 << 20
	maxQueryBytes        = 100_000
)

var errRefused = errors.New("refusal")

type client struct {
	apiKey       string
	organization string
	httpClient   *http.Client
	// inFlight is shared by every call, so it bounds the load on the API
	// however many queries run.
	inFlight chan struct{}
	// inFlightHeaderHost serves requests to a host the request header named. A
	// caller can name a host that never answers, and that must not hold the
	// slots of requests to the class or default host.
	inFlightHeaderHost chan struct{}
	logger             logrus.FieldLogger
}

func New(apiKey, organization string, timeout time.Duration, maxConcurrentRequests int, logger logrus.FieldLogger) *client {
	slots := max(maxConcurrentRequests, 1)
	return &client{
		apiKey:             apiKey,
		organization:       organization,
		httpClient:         modulecomponents.NewBaseHttpClient(timeout),
		inFlight:           make(chan struct{}, slots),
		inFlightHeaderHost: make(chan struct{}, slots),
		logger:             logger,
	}
}

// RankProperty sends one request per document.
func (c *client) RankProperty(ctx context.Context, query, property string, documents []ent.Document,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	if query == "" {
		return nil, errors.New("rerank.query is required by reranker-openai: the query is the condition the model judges")
	}
	if len(query) > maxQueryBytes {
		return nil, fmt.Errorf("rerank.query is %d bytes; reranker-openai sends it with every document and takes at most %d bytes",
			len(query), maxQueryBytes)
	}
	image, err := propertyInput(cfg, property)
	if err != nil {
		return nil, err
	}
	settings := config.NewClassSettings(cfg)
	if err := settings.CheckSettings(); err != nil {
		return nil, err
	}
	endpointURL, apiKey, hostFromHeader, err := c.endpoint(ctx, settings.BaseURL())
	if err != nil {
		return nil, err
	}

	owner := make(map[string]int, len(documents))
	var pending []int
	for i, document := range documents {
		if document.Value == "" {
			continue
		}
		if _, ok := owner[document.Value]; ok {
			continue
		}
		owner[document.Value] = i
		pending = append(pending, i)
	}

	if image {
		if err := checkImages(documents, pending); err != nil {
			return nil, err
		}
	}

	request := judgeRequest{
		endpointURL: endpointURL, apiKey: apiKey, organization: c.organizationFor(ctx, hostFromHeader),
		hostFromHeader: hostFromHeader, model: settings.Model(), query: query, image: image,
	}
	scores := make([]float64, len(documents))
	tokensPerRequest := make([]int64, len(pending))
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(cap(c.slotsFor(hostFromHeader)))
	for p, index := range pending {
		eg.Go(func() error {
			score, tokens, err := c.judge(ctx, request, documents[index])
			if err != nil {
				return err
			}
			scores[index] = score
			tokensPerRequest[p] = tokens
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	var totalInputTokens int64
	for _, tokens := range tokensPerRequest {
		totalInputTokens += tokens
	}
	c.logger.WithFields(logrus.Fields{
		"documents":    len(documents),
		"judged":       len(pending),
		"input_tokens": totalInputTokens,
	}).Debug("OpenAI rerank finished")

	documentScores := make([]ent.DocumentScore, len(documents))
	for i, document := range documents {
		score := 0.0
		if position, ok := owner[document.Value]; ok {
			score = scores[position]
		}
		documentScores[i] = ent.DocumentScore{Document: document.Value, Score: score}
	}
	return &ent.RankResult{Query: query, DocumentScores: documentScores}, nil
}

// The gRPC parser runs the same check before the search; GraphQL has none.
func propertyInput(cfg moduletools.ClassConfig, property string) (image bool, err error) {
	if cfg == nil {
		return false, errors.New("no class config")
	}
	switch cfg.PropertiesDataTypes()[property] {
	// string is deprecated since v1.19 but old classes still store it.
	case schema.DataTypeText, schema.DataTypeString:
		return false, nil
	case schema.DataTypeBlob:
		return true, nil
	default:
		return false, fmt.Errorf("property %q is not a text or blob property; reranker-openai reranks text and blob properties", property)
	}
}

type judgeRequest struct {
	endpointURL  string
	apiKey       string
	organization string
	// hostFromHeader says the request header named the host. Anyone who may
	// query can name one, so status errors quote no reply body for it. A wrong
	// answer name or type is quoted (cut to 512 bytes) for every host.
	hostFromHeader bool
	model          string
	query          string
	image          bool
}

// judge names the document's object in a 400 and in a rejected answer.
func (c *client) judge(ctx context.Context, r judgeRequest, document ent.Document) (float64, int64, error) {
	response, err := c.send(ctx, r, document.Value)
	if err != nil {
		var rejected rejectedError
		if errors.As(err, &rejected) && rejected.status == http.StatusBadRequest {
			return 0, 0, fmt.Errorf("object %s: %w", document.ID, err)
		}
		return 0, 0, err
	}
	score, err := response.score(questionName)
	if errors.Is(err, errRefused) {
		err = fmt.Errorf("OpenAI refused to judge the condition %q", truncate(r.query))
	}
	if err != nil {
		return 0, 0, fmt.Errorf("object %s: %w", document.ID, err)
	}
	return score, inputTokens(response.Usage), nil
}

func (c *client) slotsFor(hostFromHeader bool) chan struct{} {
	if hostFromHeader {
		return c.inFlightHeaderHost
	}
	return c.inFlight
}

// send builds the input inside the in-flight slot, so the slots also bound the
// image copies in memory.
func (c *client) send(ctx context.Context, r judgeRequest, document string) (decisionResponse, error) {
	// select picks randomly among ready cases, so a free slot could win over a
	// cancellation.
	if err := ctx.Err(); err != nil {
		return decisionResponse{}, err
	}
	slots := c.slotsFor(r.hostFromHeader)
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return decisionResponse{}, ctx.Err()
	}

	input, err := newInput(document, r.image)
	if err != nil {
		return decisionResponse{}, fmt.Errorf("image: %w", err)
	}
	body, err := json.Marshal(decisionRequest{
		Model:     r.model,
		Input:     input,
		Questions: []decisionQuestion{{Type: predicateType, Name: questionName, Instructions: r.query}},
	})
	if err != nil {
		return decisionResponse{}, fmt.Errorf("marshal OpenAI request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpointURL, bytes.NewReader(body))
	if err != nil {
		return decisionResponse{}, fmt.Errorf("create OpenAI request: %w", err)
	}
	req.Header.Add("Authorization", "Bearer "+r.apiKey)
	req.Header.Add("Content-Type", "application/json")
	if r.organization != "" {
		req.Header.Add("OpenAI-Organization", r.organization)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return decisionResponse{}, fmt.Errorf("send OpenAI request: %w", err)
	}
	defer res.Body.Close()
	return readResponse(res, r.hostFromHeader)
}

// readResponse quotes no status error body and no error object of a host the
// request header named. A decode error is cut to 512 bytes for every host.
func readResponse(res *http.Response, hostFromHeader bool) (decisionResponse, error) {
	if res.StatusCode != http.StatusOK {
		apiErr := readAPIError(res.Body)
		if hostFromHeader {
			apiErr = apiError{}
		}
		return decisionResponse{}, statusError(res.StatusCode, apiErr)
	}

	body, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBodyBytes+1))
	if err != nil {
		return decisionResponse{}, fmt.Errorf("read OpenAI response: %w", err)
	}
	if len(body) > maxResponseBodyBytes {
		return decisionResponse{}, fmt.Errorf("OpenAI response exceeds %d bytes", maxResponseBodyBytes)
	}
	var response decisionResponse
	if err := json.Unmarshal(body, &response); err != nil {
		err = fmt.Errorf("parse OpenAI response: %s", truncate(err.Error()))
		if hostFromHeader || len(body) == 0 {
			return decisionResponse{}, err
		}
		return decisionResponse{}, fmt.Errorf("%w: %s", err, truncate(string(body)))
	}
	if response.Error != nil {
		message := response.Error.message()
		if hostFromHeader || message == "" {
			return decisionResponse{}, errors.New("OpenAI API returned an error")
		}
		return decisionResponse{}, fmt.Errorf("OpenAI API returned an error: %s", message)
	}
	return response, nil
}

type apiError struct {
	Message string `json:"message"`
}

func (e apiError) message() string {
	return truncate(e.Message)
}

func readAPIError(body io.Reader) apiError {
	var parsed struct {
		Error apiError `json:"error"`
	}
	errorBody, _ := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes))
	if json.Unmarshal(errorBody, &parsed) != nil {
		return apiError{}
	}
	return parsed.Error
}

func statusError(status int, apiErr apiError) error {
	// OpenAI's message for a rejected key holds a masked form of the key.
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return fmt.Errorf("OpenAI rejected the API key (status %d)", status)
	}
	return rejectedError{status: status, message: apiErr.message()}
}

type rejectedError struct {
	status  int
	message string
}

func (e rejectedError) Error() string {
	if e.message == "" {
		return fmt.Sprintf("connection to OpenAI API failed with status %d", e.status)
	}
	return fmt.Sprintf("connection to OpenAI API failed with status %d: %s", e.status, e.message)
}

func truncate(s string) string {
	if len(s) <= maxErrorMessageBytes {
		return s
	}
	return strings.ToValidUTF8(s[:maxErrorMessageBytes], "")
}

// The server's organization never goes to a host the request header named, as
// its key does not.
func (c *client) organizationFor(ctx context.Context, hostFromHeader bool) string {
	if organization := modulecomponents.GetValueFromContext(ctx, organizationHeader); organization != "" {
		return organization
	}
	if hostFromHeader {
		return ""
	}
	return c.organization
}

// A base URL from the request header is used only with a key from the request
// header, so the server's key never goes to a host the request names.
func (c *client) endpoint(ctx context.Context, classBaseURL string,
) (endpointURL, apiKey string, hostFromHeader bool, err error) {
	apiKey = modulecomponents.GetValueFromContext(ctx, apiKeyHeader)
	baseURL := classBaseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, baseURLHeader); headerBaseURL != "" {
		if apiKey == "" {
			return "", "", false, fmt.Errorf("%s needs %s: the server's key is not sent to a host named by the request",
				baseURLHeader, apiKeyHeader)
		}
		normalized, err := config.NormalizeBaseURL(headerBaseURL)
		if err != nil {
			return "", "", false, fmt.Errorf("%s header %w", baseURLHeader, err)
		}
		if err := modulecomponents.ValidateBaseURL(normalized); err != nil {
			return "", "", false, err
		}
		baseURL = normalized
		hostFromHeader = true
	}
	if apiKey == "" {
		apiKey = c.apiKey
	}
	if apiKey == "" {
		return "", "", false, fmt.Errorf("OpenAI API key: no api key found neither in request header: %s "+
			"nor in environment variable under OPENAI_APIKEY", apiKeyHeader)
	}
	endpointURL, err = url.JoinPath(baseURL, apiPath)
	if err != nil {
		return "", "", false, fmt.Errorf("OpenAI base URL %q: %w", baseURL, err)
	}
	return endpointURL, apiKey, hostFromHeader, nil
}

type decisionRequest struct {
	Model     string             `json:"model"`
	Input     any                `json:"input"`
	Questions []decisionQuestion `json:"questions"`
}

type decisionQuestion struct {
	Type string `json:"type"`
	// Name is echoed by the API in its answer.
	Name         string `json:"name"`
	Instructions string `json:"instructions"`
}

type decisionResponse struct {
	Answers []decisionAnswer `json:"answers"`
	// Usage is decoded apart, so that a usage of another shape does not fail
	// the answers.
	Usage json.RawMessage `json:"usage"`
	Error *apiError       `json:"error"`
}

// Probability is a pointer so that a missing value does not read as 0.
type decisionAnswer struct {
	Type        string   `json:"type"`
	Name        string   `json:"name"`
	Probability *float64 `json:"probability"`
}

func inputTokens(usage json.RawMessage) int64 {
	var u struct {
		InputTokens json.Number `json:"input_tokens"`
	}
	if json.Unmarshal(usage, &u) != nil {
		return 0
	}
	tokens, err := u.InputTokens.Float64()
	if err != nil {
		return 0
	}
	return int64(tokens)
}

func (r decisionResponse) score(name string) (float64, error) {
	if len(r.Answers) != 1 {
		return 0, fmt.Errorf("OpenAI response has %d answers, want 1", len(r.Answers))
	}
	answer := r.Answers[0]
	if answer.Name != name {
		return 0, fmt.Errorf("OpenAI answer is for %q, want %q", truncate(answer.Name), name)
	}
	switch answer.Type {
	case refusalType:
		return 0, errRefused
	case predicateType:
		if answer.Probability == nil {
			return 0, errors.New("OpenAI answer has no probability")
		}
		if p := *answer.Probability; p < 0 || p > 1 {
			return 0, fmt.Errorf("OpenAI answer has probability %v, outside [0, 1]", p)
		}
		return *answer.Probability, nil
	default:
		return 0, fmt.Errorf("OpenAI answer has type %q, want %q", truncate(answer.Type), predicateType)
	}
}
