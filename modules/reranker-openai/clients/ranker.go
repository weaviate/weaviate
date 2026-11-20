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
	"runtime"
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

var _NUMCPU = runtime.NumCPU()

const (
	// MaxDocumentsPerQuery is 1,000 documents. A query sends one billed
	// request per object.
	MaxDocumentsPerQuery = 1000

	apiPath            = "/v1/decisions"
	predicateType      = "predicate"
	refusalType        = "refusal"
	questionName       = "condition"
	apiKeyHeader       = "X-Openai-Api-Key"
	baseURLHeader      = "X-Openai-Baseurl"
	organizationHeader = "X-Openai-Organization"
)

type client struct {
	apiKey       string
	organization string
	httpClient   *http.Client
	// maxDocumentsPerQuery counts the documents that are sent: blank and
	// duplicate documents are not.
	maxDocumentsPerQuery int
	logger               logrus.FieldLogger
}

func New(apiKey, organization string, timeout time.Duration, maxDocumentsPerQuery int, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:               apiKey,
		organization:         organization,
		httpClient:           modulecomponents.NewBaseHttpClient(timeout),
		maxDocumentsPerQuery: max(maxDocumentsPerQuery, 1),
		logger:               logger,
	}
}

// validateQuery rejects a blank rerank.query. The gRPC search rejects it
// before the search; GraphQL reaches this check only. The API answers a blank
// query with a probability and bills the request.
func (c *client) validateQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return errors.New("rerank.query is required by reranker-openai: the query is the condition the model judges")
	}
	return nil
}

// Rank judges each document in its own request. A blank document and
// a duplicate of an earlier document have none.
//
// A reply with an error status, such as a 429 or a 500, fails the query, and
// its request is not sent again. The shared transport sends a request again
// when a connection from its pool breaks before a reply arrives. The host can
// then receive the same request more than once.
func (c *client) Rank(ctx context.Context, query, property string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	if err := c.validateQuery(query); err != nil {
		return nil, err
	}
	image, err := c.propertyInput(cfg, property)
	if err != nil {
		return nil, err
	}
	settings := config.NewClassSettings(cfg)
	endpointURL, apiKey, err := c.endpoint(ctx, settings.BaseURL())
	if err != nil {
		return nil, err
	}

	owner := make(map[string]int, len(documents))
	var pending []int
	for i, document := range documents {
		if strings.TrimSpace(document) == "" {
			continue
		}
		if _, ok := owner[document]; ok {
			continue
		}
		owner[document] = i
		pending = append(pending, i)
	}
	if len(pending) > c.maxDocumentsPerQuery {
		return nil, fmt.Errorf("rerank of %d objects: reranker-openai sends one request per object and takes at most %d per query",
			len(pending), c.maxDocumentsPerQuery)
	}

	if image {
		if err := c.checkImages(documents, pending); err != nil {
			return nil, err
		}
	}

	request := judgeRequest{
		endpointURL: endpointURL, apiKey: apiKey, organization: c.organizationFor(ctx),
		model: settings.Model(), query: query, image: image,
	}
	scores := make([]float64, len(documents))
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(_NUMCPU)
	for _, index := range pending {
		eg.Go(func() error {
			// After a failed request, the rest would still build their bodies.
			if err := ctx.Err(); err != nil {
				return err
			}
			score, err := c.judge(ctx, request, documents[index])
			if err != nil {
				return err
			}
			scores[index] = score
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	documentScores := make([]ent.DocumentScore, len(documents))
	for i, document := range documents {
		score := 0.0
		if position, ok := owner[document]; ok {
			score = scores[position]
		}
		documentScores[i] = ent.DocumentScore{Document: document, Score: score}
	}
	return &ent.RankResult{Query: query, DocumentScores: documentScores}, nil
}

// The gRPC parser runs the same check before the search; GraphQL has none.
func (c *client) propertyInput(cfg moduletools.ClassConfig, property string) (image bool, err error) {
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
	model        string
	query        string
	image        bool
}

func (c *client) judge(ctx context.Context, r judgeRequest, document string) (float64, error) {
	response, err := c.send(ctx, r, document)
	if err != nil {
		return 0, err
	}
	return response.score(questionName)
}

func (c *client) send(ctx context.Context, r judgeRequest, document string) (decisionResponse, error) {
	input, err := r.input(document)
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
	return c.readResponse(res)
}

func (c *client) readResponse(res *http.Response) (decisionResponse, error) {
	if res.StatusCode != http.StatusOK {
		return decisionResponse{}, c.statusError(res)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return decisionResponse{}, fmt.Errorf("read OpenAI response: %w", err)
	}
	var response decisionResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return decisionResponse{}, fmt.Errorf("parse OpenAI response: %w: %s", err, body)
	}
	if response.Error != nil {
		message := response.Error.Message
		if message == "" {
			message = string(body)
		}
		return decisionResponse{}, fmt.Errorf("OpenAI API returned an error: %s", message)
	}
	return response, nil
}

type apiError struct {
	Message string
}

// UnmarshalJSON takes the error object of OpenAI and an error that is a
// string, which some OpenAI-compatible servers send. A message that is not a
// string reads as no message.
func (e *apiError) UnmarshalJSON(data []byte) error {
	var message string
	if json.Unmarshal(data, &message) == nil {
		e.Message = message
		return nil
	}
	var fields struct {
		Message any `json:"message"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	e.Message, _ = fields.Message.(string)
	return nil
}

// statusError quotes no body for a 401 or a 403. OpenAI's message for a
// rejected key holds a masked form of the key, and its message for a 403 can
// name the project of the key. Any other status quotes the message of the
// error object, or the reply as it is.
func (c *client) statusError(res *http.Response) error {
	switch res.StatusCode {
	case http.StatusUnauthorized:
		return errors.New("OpenAI rejected the API key (status 401)")
	case http.StatusForbidden:
		return errors.New("OpenAI denied the request (status 403)")
	}
	body, _ := io.ReadAll(res.Body)
	message := string(body)
	var parsed struct {
		Error apiError `json:"error"`
	}
	if json.Unmarshal(body, &parsed) == nil && parsed.Error.Message != "" {
		message = parsed.Error.Message
	}
	return rejectedError{status: res.StatusCode, message: message}
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

func (c *client) organizationFor(ctx context.Context) string {
	if organization := modulecomponents.GetValueFromContext(ctx, organizationHeader); organization != "" {
		return organization
	}
	return c.organization
}

// endpoint picks the base URL and the key of a query.
func (c *client) endpoint(ctx context.Context, classBaseURL string) (endpointURL, apiKey string, err error) {
	baseURL, err := modulecomponents.ValidatedBaseURLFromHeader(ctx, baseURLHeader, classBaseURL)
	if err != nil {
		return "", "", err
	}
	apiKey = modulecomponents.GetValueFromContext(ctx, apiKeyHeader)
	if apiKey == "" {
		apiKey = c.apiKey
	}
	if apiKey == "" {
		return "", "", fmt.Errorf("OpenAI API key: no api key found neither in request header: %s "+
			"nor in environment variable under OPENAI_APIKEY", apiKeyHeader)
	}
	endpointURL, err = url.JoinPath(baseURL, apiPath)
	if err != nil {
		return "", "", fmt.Errorf("OpenAI base URL %q: %w", baseURL, err)
	}
	return endpointURL, apiKey, nil
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
	Error   *apiError        `json:"error"`
}

// Probability is a pointer so that a missing value does not read as 0.
type decisionAnswer struct {
	Type        string   `json:"type"`
	Name        string   `json:"name"`
	Probability *float64 `json:"probability"`
}

func (r decisionResponse) score(name string) (float64, error) {
	if len(r.Answers) != 1 {
		return 0, fmt.Errorf("OpenAI response has %d answers, want 1", len(r.Answers))
	}
	answer := r.Answers[0]
	if answer.Name != name {
		return 0, fmt.Errorf("OpenAI answer is for %q, want %q", answer.Name, name)
	}
	switch answer.Type {
	case refusalType:
		// OpenAI refuses a document whose content has no evidence for the
		// condition, so the condition does not hold for it.
		return 0, nil
	case predicateType:
		if answer.Probability == nil {
			return 0, errors.New("OpenAI answer has no probability")
		}
		if p := *answer.Probability; p < 0 || p > 1 {
			return 0, fmt.Errorf("OpenAI answer has probability %v, outside [0, 1]", p)
		}
		return *answer.Probability, nil
	default:
		return 0, fmt.Errorf("OpenAI answer has type %q, want %q", answer.Type, predicateType)
	}
}
