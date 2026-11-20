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
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/reranker-typesafeai/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

var _NUMCPU = runtime.NumCPU()

const (
	// maxBatchDocuments is 50 texts per request: beyond that a request saves
	// almost no tokens and answers more slowly than two sent in parallel. A
	// request the API rejects for its size is sent again as two halves.
	// Images go one per request.
	maxBatchDocuments = 50

	apiPath       = "/v1/systemone"
	questionKey   = "q"
	noulType      = "noul"
	apiKeyHeader  = "X-Typesafeai-Api-Key"
	baseURLHeader = "X-Typesafeai-Baseurl"
	// tooLargeError is the error_type of the 400 the API answers to a request
	// above its token limit.
	tooLargeError = "max_tokens_exceeded"
)

type client struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:     apiKey,
		httpClient: modulecomponents.NewBaseHttpClient(timeout),
		logger:     logger,
	}
}

// validateQuery rejects a blank rerank.query. The gRPC search rejects it
// before the search; GraphQL reaches this check only. The API answers 400 to
// an empty query and scores the documents against a query of whitespace.
func (c *client) validateQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return errors.New("rerank.query is required by reranker-typesafeai: the query is the condition the model judges")
	}
	return nil
}

// Rank returns, per document, the probability that the query holds for it.
// Documents that share a request influence each other's probability a
// little.
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
	if image {
		if err := c.checkImages(documents, pending); err != nil {
			return nil, err
		}
	}

	batchSize := maxBatchDocuments
	if image {
		batchSize = 1
	}
	request := judgeRequest{url: endpointURL, apiKey: apiKey, model: settings.Model(), query: query, image: image}
	scores := make([]float64, len(documents))
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(_NUMCPU)
	for _, batch := range c.batches(pending, batchSize) {
		eg.Go(func() error {
			// After a failed batch, the rest would still build their bodies.
			if err := ctx.Err(); err != nil {
				return err
			}
			batchDocuments := make([]string, len(batch))
			for j, index := range batch {
				batchDocuments[j] = documents[index]
			}
			probabilities, err := c.judge(ctx, request, batchDocuments)
			if err != nil {
				return err
			}
			for j, index := range batch {
				scores[index] = probabilities[j]
			}
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

// propertyInput says whether the documents of the property are images. The
// gRPC parser runs the same check before the search; GraphQL has none and
// reaches this one only.
func (c *client) propertyInput(cfg moduletools.ClassConfig, property string) (image bool, err error) {
	if cfg == nil {
		return false, errors.New("no class config")
	}
	switch cfg.PropertiesDataTypes()[property] {
	// string is the text type deprecated in v1.19 that old classes still store.
	case schema.DataTypeText, schema.DataTypeString:
		return false, nil
	case schema.DataTypeBlob:
		return true, nil
	default:
		return false, fmt.Errorf("property %q is not a text or blob property; reranker-typesafeai reranks text and blob properties", property)
	}
}

// batches splits the positions of the documents into requests of at most
// size documents, in order.
func (c *client) batches(indexes []int, size int) [][]int {
	var out [][]int
	for start := 0; start < len(indexes); start += size {
		out = append(out, indexes[start:min(start+size, len(indexes))])
	}
	return out
}

// judgeRequest is what the requests of one call have in common.
type judgeRequest struct {
	url    string
	apiKey string
	model  string
	query  string
	image  bool
}

// judge returns the probability of every document, in order. A request the
// API rejects for its size is sent again as two halves, down to one document.
func (c *client) judge(ctx context.Context, r judgeRequest, documents []string) ([]float64, error) {
	response, tooLarge, err := c.post(ctx, r, documents)
	if tooLarge && len(documents) > 1 {
		half := len(documents) / 2
		first, err := c.judge(ctx, r, documents[:half])
		if err != nil {
			return nil, err
		}
		second, err := c.judge(ctx, r, documents[half:])
		if err != nil {
			return nil, err
		}
		return append(first, second...), nil
	}
	if err != nil {
		return nil, err
	}
	return response.values(len(documents))
}

// post builds and sends the request of one batch.
func (c *client) post(ctx context.Context, r judgeRequest, documents []string) (typesafeaiResponse, bool, error) {
	request, err := r.request(documents)
	if err != nil {
		return typesafeaiResponse{}, false, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return typesafeaiResponse{}, false, fmt.Errorf("marshal TypeSafe AI request: %w", err)
	}
	return c.send(ctx, r.url, r.apiKey, body, r.image)
}

// send does not retry a status: a 429 or a 5xx fails the call. The transport
// sends the request again, up to twice, when a reused connection breaks before
// a response arrives, so the API can receive a request three times. tooLarge
// says the API rejected the request for its size.
func (c *client) send(ctx context.Context, endpointURL, apiKey string, body []byte, images bool,
) (response typesafeaiResponse, tooLarge bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(body))
	if err != nil {
		return typesafeaiResponse{}, false, fmt.Errorf("create TypeSafe AI request: %w", err)
	}
	req.Header.Add("Authorization", "Bearer "+apiKey)
	req.Header.Add("Content-Type", "application/json")
	if images {
		req.Header.Add(dangerZoneImagesHeader, "true")
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return typesafeaiResponse{}, false, fmt.Errorf("send TypeSafe AI request: %w", err)
	}
	defer res.Body.Close()

	reply, err := io.ReadAll(res.Body)
	if err != nil {
		return typesafeaiResponse{}, false, fmt.Errorf("read TypeSafe AI response: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		tooLarge = res.StatusCode == http.StatusBadRequest && bytes.Contains(reply, []byte(tooLargeError))
		return typesafeaiResponse{}, tooLarge, apiError{status: res.StatusCode, message: string(reply)}
	}
	if err := json.Unmarshal(reply, &response); err != nil {
		return typesafeaiResponse{}, false, fmt.Errorf("parse TypeSafe AI response: %w: %s", err, reply)
	}
	// The shape of an error in a 200 is not documented, so the body is the
	// provider's message.
	if len(response.Answers) == 0 {
		return typesafeaiResponse{}, false, fmt.Errorf("TypeSafe AI response has no answers: %s", reply)
	}
	return response, false, nil
}

// apiError is a status other than 200, with the reply body as its message.
type apiError struct {
	status  int
	message string
}

func (e apiError) Error() string {
	if e.message == "" {
		return fmt.Sprintf("connection to TypeSafe AI API failed with status %d", e.status)
	}
	return fmt.Sprintf("connection to TypeSafe AI API failed with status %d: %s", e.status, e.message)
}

// endpoint returns the URL and the key of the requests.
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
		return "", "", fmt.Errorf("TypeSafe AI API key: no api key found neither in request header: %s "+
			"nor in environment variable under TYPESAFEAI_APIKEY", apiKeyHeader)
	}
	endpointURL, err = url.JoinPath(baseURL, apiPath)
	if err != nil {
		return "", "", fmt.Errorf("TypeSafe AI base URL %q: %w", baseURL, err)
	}
	return endpointURL, apiKey, nil
}

type typesafeaiRequest struct {
	Model     string                        `json:"model"`
	State     any                           `json:"state"`
	Questions map[string]typesafeaiQuestion `json:"questions"`
	Assets    map[string]imageAsset         `json:"assets,omitempty"`
}

type typesafeaiQuestion struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions"`
}

// request asks about a single document with the query alone. Several
// documents become a map with one question per document, which names the
// document it is about. An image is an asset and the state holds its
// reference.
func (r judgeRequest) request(documents []string) (typesafeaiRequest, error) {
	values := documents
	var assets map[string]imageAsset
	if r.image {
		var err error
		assets, values, err = r.assets(documents)
		if err != nil {
			return typesafeaiRequest{}, err
		}
	}
	if len(documents) == 1 {
		return typesafeaiRequest{
			Model:     r.model,
			State:     values[0],
			Questions: map[string]typesafeaiQuestion{questionKey: {Type: noulType, Instructions: r.query}},
			Assets:    assets,
		}, nil
	}
	state := make(map[string]string, len(documents))
	questions := make(map[string]typesafeaiQuestion, len(documents))
	for i, value := range values {
		key := documentKey(i)
		state[key] = value
		questions[key] = typesafeaiQuestion{Type: noulType, Instructions: "In " + key + ": " + r.query}
	}
	return typesafeaiRequest{Model: r.model, State: state, Questions: questions, Assets: assets}, nil
}

func documentKey(i int) string {
	return "document_" + strconv.Itoa(i)
}

type typesafeaiResponse struct {
	Answers map[string]typesafeaiAnswer `json:"answers"`
}

type typesafeaiAnswer struct {
	Type string   `json:"type"`
	Noul *float64 `json:"noul"`
}

// values returns the probabilities of a request of n documents, in order.
func (r typesafeaiResponse) values(n int) ([]float64, error) {
	if len(r.Answers) != n {
		return nil, fmt.Errorf("TypeSafe AI response has %d answers for %d documents", len(r.Answers), n)
	}
	if n == 1 {
		value, err := r.value(questionKey)
		if err != nil {
			return nil, err
		}
		return []float64{value}, nil
	}
	out := make([]float64, n)
	for i := range out {
		value, err := r.value(documentKey(i))
		if err != nil {
			return nil, err
		}
		out[i] = value
	}
	return out, nil
}

func (r typesafeaiResponse) value(key string) (float64, error) {
	answer, ok := r.Answers[key]
	if !ok {
		return 0, fmt.Errorf("TypeSafe AI response has no answer for %q", key)
	}
	if answer.Type != noulType {
		return 0, fmt.Errorf("TypeSafe AI answer for %q has type %q, want %q", key, answer.Type, noulType)
	}
	if answer.Noul == nil {
		return 0, fmt.Errorf("TypeSafe AI answer for %q has no probability", key)
	}
	if p := *answer.Noul; p < 0 || p > 1 {
		return 0, fmt.Errorf("TypeSafe AI answer for %q has probability %v, outside [0, 1]", key, p)
	}
	return *answer.Noul, nil
}
