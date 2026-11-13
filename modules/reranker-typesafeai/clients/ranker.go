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

const (
	// DefaultMaxConcurrentRequests is the default size of each of the two
	// pools of the client, so a process has up to twice as many requests in
	// flight. A request with images holds two to four times their base64
	// while it holds a slot, up to 24 MiB of base64 per request.
	DefaultMaxConcurrentRequests = 16
	MaxConcurrentRequestsLimit   = 256

	// The API does not publish its request size limit: 141,000 characters
	// were accepted and 149,000 rejected. A batch closes before the bytes of
	// its documents plus their questions, each of which repeats the query,
	// would pass MaxBatchCharacters. One document above the limit is sent
	// alone. A non-ASCII character takes several bytes, so a batch of such
	// text holds fewer characters than the limit. Beyond 50 documents a
	// request saves almost no tokens and answers more slowly than two sent in
	// parallel.
	MaxBatchDocuments  = 50
	MaxBatchCharacters = 100_000

	apiPath       = "/v1/systemone"
	questionKey   = "q"
	noulType      = "noul"
	apiKeyHeader  = "X-Typesafeai-Api-Key"
	baseURLHeader = "X-Typesafeai-Baseurl"
	// tooLargeError is the error_type of the 400 the API answers to a request
	// above its token limit.
	tooLargeError        = "max_tokens_exceeded"
	maxErrorBodyBytes    = 512
	maxResponseBodyBytes = 1 << 20
)

type client struct {
	apiKey     string
	httpClient *http.Client
	// inFlight holds a slot per request to a class or default host being
	// built or sent. Every call and every such host share it, so it bounds
	// the load on the API and the request bodies in memory however many
	// queries run. A class baseURL host that never answers holds its slots
	// until the client timeout, and the queries of every other collection
	// wait for them.
	inFlight chan struct{}
	// inFlightHeaderHost is the pool of requests to a host the request header
	// named. It has the size of inFlight and every such host shares it. A
	// caller can name a host that never answers, and that must not hold the
	// slots of requests to a class or default host.
	inFlightHeaderHost chan struct{}
	logger             logrus.FieldLogger
}

// New returns a client with two pools of maxConcurrentRequests slots: one
// for the requests to class and default hosts, one for the requests to hosts
// the request header names. Up to twice maxConcurrentRequests requests are in
// flight at once.
func New(apiKey string, timeout time.Duration, maxConcurrentRequests int, logger logrus.FieldLogger) *client {
	slots := max(maxConcurrentRequests, 1)
	return &client{
		apiKey: apiKey,
		// Each slot keeps its connection between queries.
		httpClient:         modulecomponents.NewBaseHttpClientWithIdleConns(timeout, slots),
		inFlight:           make(chan struct{}, slots),
		inFlightHeaderHost: make(chan struct{}, slots),
		logger:             logger,
	}
}

// ValidateQuery returns the error of a rerank.query the module does not rank
// with: an empty one, and one whose question for a document does not fit in
// a request.
func ValidateQuery(query string) error {
	if query == "" {
		return errors.New("rerank.query is required by reranker-typesafeai: the query is the condition the model judges")
	}
	if questionBytes(query) >= MaxBatchCharacters {
		return fmt.Errorf("rerank.query is %d bytes; reranker-typesafeai repeats it for every document within %d bytes per request",
			len(query), MaxBatchCharacters)
	}
	return nil
}

// questionBytes is the size of the longest question a request repeats the
// query in.
func questionBytes(query string) int {
	return len("In "+documentKey(MaxBatchDocuments-1)+": ") + len(query)
}

// RankProperty returns, per document, the probability that the query holds
// for it. Documents that share a request influence each other's probability
// a little.
func (c *client) RankProperty(ctx context.Context, query, property string, documents []ent.Document,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	if err := ValidateQuery(query); err != nil {
		return nil, err
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
		if strings.TrimSpace(document.Value) == "" {
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

	maxDocuments, maxCharacters := MaxBatchDocuments, MaxBatchCharacters
	if image {
		maxDocuments, maxCharacters = MaxBatchImages, MaxBatchImageChars
	}
	request := judgeRequest{
		url: endpointURL, apiKey: apiKey, hostFromHeader: hostFromHeader,
		model: settings.Model(), query: query, image: image,
	}
	requestBatches := batches(pending, documents, maxDocuments, maxCharacters, questionBytes(query))
	scores := make([]float64, len(documents))
	usages := make([]typesafeaiUsage, len(requestBatches))
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(cap(c.slots(hostFromHeader)))
	for b, batch := range requestBatches {
		eg.Go(func() error {
			batchDocuments := make([]ent.Document, len(batch))
			for j, index := range batch {
				batchDocuments[j] = documents[index]
			}
			probabilities, usage, err := c.judge(ctx, request, batchDocuments, false)
			if err != nil {
				return err
			}
			for j, index := range batch {
				scores[index] = probabilities[j]
			}
			usages[b] = usage
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	var total typesafeaiUsage
	for _, usage := range usages {
		total = total.add(usage)
	}
	c.logger.WithFields(logrus.Fields{
		"documents":    len(documents),
		"judged":       len(pending),
		"requests":     total.requests,
		"input_tokens": total.InputTokens,
	}).Debug("TypeSafe AI rerank finished")

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

// propertyInput says whether the documents of the property are images. The
// gRPC parser runs the same check before the search; GraphQL has none and
// reaches this one only.
func propertyInput(cfg moduletools.ClassConfig, property string) (image bool, err error) {
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
// maxDocuments documents and maxCharacters bytes, in order. A document costs
// its bytes plus questionBytes. A document that costs more than maxCharacters
// goes alone.
func batches(indexes []int, documents []ent.Document, maxDocuments, maxCharacters, questionBytes int) [][]int {
	var out [][]int
	var batch []int
	characters := 0
	for _, index := range indexes {
		length := len(documents[index].Value) + questionBytes
		if len(batch) > 0 && (len(batch) == maxDocuments || characters+length > maxCharacters) {
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

// judgeRequest is what the requests of one call have in common.
type judgeRequest struct {
	url    string
	apiKey string
	// hostFromHeader says the request header named the host. Anyone who may
	// query can name one, so the errors then quote nothing of its replies.
	hostFromHeader bool
	model          string
	query          string
	image          bool
}

// judge returns the probability of every document, in order. A request the
// API rejects for its size is sent again as two halves, down to one document.
// A 400 to a request of one document names its object when the 400 says the
// document is too large or a split isolated the document; any other 400 to a
// page of one document can be about the request, such as an unknown model.
func (c *client) judge(ctx context.Context, r judgeRequest, documents []ent.Document, split bool,
) ([]float64, typesafeaiUsage, error) {
	response, tooLarge, err := c.post(ctx, r, documents)
	if tooLarge && len(documents) > 1 {
		half := len(documents) / 2
		first, firstUsage, err := c.judge(ctx, r, documents[:half], true)
		if err != nil {
			return nil, typesafeaiUsage{}, err
		}
		second, secondUsage, err := c.judge(ctx, r, documents[half:], true)
		if err != nil {
			return nil, typesafeaiUsage{}, err
		}
		return append(first, second...), typesafeaiUsage{requests: 1}.add(firstUsage).add(secondUsage), nil
	}
	if err != nil {
		var rejected apiError
		if len(documents) == 1 && (tooLarge || split) && errors.As(err, &rejected) && rejected.status == http.StatusBadRequest {
			return nil, typesafeaiUsage{}, fmt.Errorf("object %s: %w", documents[0].ID, err)
		}
		return nil, typesafeaiUsage{}, err
	}
	values, err := response.values(len(documents))
	if err != nil {
		return nil, typesafeaiUsage{}, err
	}
	return values, typesafeaiUsage{InputTokens: inputTokens(response.Usage), requests: 1}, nil
}

// slots returns the pool a request to the host takes its slot from.
func (c *client) slots(hostFromHeader bool) chan struct{} {
	if hostFromHeader {
		return c.inFlightHeaderHost
	}
	return c.inFlight
}

// post builds and sends the request of one batch while it holds an in-flight
// slot, so that the slots also bound the request bodies in memory.
func (c *client) post(ctx context.Context, r judgeRequest, documents []ent.Document) (typesafeaiResponse, bool, error) {
	// select picks at random among ready cases, so a free slot could win over
	// a cancellation.
	if err := ctx.Err(); err != nil {
		return typesafeaiResponse{}, false, err
	}
	slots := c.slots(r.hostFromHeader)
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return typesafeaiResponse{}, false, ctx.Err()
	}

	request, err := newRequest(r, documents)
	if err != nil {
		return typesafeaiResponse{}, false, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return typesafeaiResponse{}, false, fmt.Errorf("marshal TypeSafe AI request: %w", err)
	}
	return c.send(ctx, r.url, r.apiKey, body, r.image, r.hostFromHeader)
}

// send does not retry a status: a 429 or a 5xx fails the call. The transport
// sends the request again when a reused connection breaks before a response
// arrives, so the API can receive a request twice. tooLarge says the API
// rejected the request for its size.
func (c *client) send(ctx context.Context, endpointURL, apiKey string, body []byte, images, hostFromHeader bool,
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
		return typesafeaiResponse{}, false, transportError(ctx, "send TypeSafe AI request",
			"no HTTP response from the host named by "+baseURLHeader, err, hostFromHeader)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		errorBody, _ := io.ReadAll(io.LimitReader(res.Body, maxErrorBodyBytes))
		tooLarge = res.StatusCode == http.StatusBadRequest && bytes.Contains(errorBody, []byte(tooLargeError))
		rejected := apiError{status: res.StatusCode}
		if !hostFromHeader {
			rejected.message = truncate(errorBody)
		}
		return typesafeaiResponse{}, tooLarge, rejected
	}

	responseBody, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBodyBytes+1))
	if err != nil {
		return typesafeaiResponse{}, false, transportError(ctx, "read TypeSafe AI response",
			"incomplete response from the host named by "+baseURLHeader, err, hostFromHeader)
	}
	if len(responseBody) > maxResponseBodyBytes {
		return typesafeaiResponse{}, false, fmt.Errorf("TypeSafe AI response exceeds %d bytes", maxResponseBodyBytes)
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		// Value holds the number literal of the reply when it is out of range,
		// and Field the keys of its answers.
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			typeErr.Value, _, _ = strings.Cut(typeErr.Value, " ")
			typeErr.Struct, typeErr.Field = "", ""
		}
		return typesafeaiResponse{}, false, withBody(fmt.Errorf("parse TypeSafe AI response: %s", truncate(err.Error())),
			responseBody, hostFromHeader)
	}
	// The shape of an error in a 200 is not documented, so the body is the
	// provider's message.
	if len(response.Answers) == 0 {
		return typesafeaiResponse{}, false, withBody(errors.New("TypeSafe AI response has no answers"),
			responseBody, hostFromHeader)
	}
	return response, false, nil
}

// transportError wraps err for the class or default host. For a host the
// request header named it returns headerHostMessage instead: net/http can
// quote bytes the host sent, and a dial or read error names the address it
// resolved to. An error of the context or of the client timeout stays
// identifiable with errors.Is.
func transportError(ctx context.Context, action, headerHostMessage string, err error, hostFromHeader bool) error {
	if !hostFromHeader {
		return fmt.Errorf("%s: %w", action, err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%s: %w", action, ctxErr)
	}
	for _, kept := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, kept) {
			return fmt.Errorf("%s: %w", action, kept)
		}
	}
	return fmt.Errorf("%s: %s", action, headerHostMessage)
}

// withBody appends the start of the body to err, unless the request header
// named the host.
func withBody(err error, body []byte, hostFromHeader bool) error {
	if hostFromHeader || len(body) == 0 {
		return err
	}
	return fmt.Errorf("%w: %s", err, truncate(body))
}

// truncate cuts s to maxErrorBodyBytes and drops the bytes that are not valid
// UTF-8, such as those of a character the cut splits.
func truncate[T string | []byte](s T) string {
	return strings.ToValidUTF8(string(s[:min(len(s), maxErrorBodyBytes)]), "")
}

// apiError is a status other than 200. message is empty for a host the
// request header named.
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

// endpoint returns the URL and the key of the requests, and whether the
// request header named the host. A base URL from the request header is used
// only with a key from the request header, so the server's key never goes to
// a host the request names. A header base URL whose normalized form equals
// classBaseURL names the host of the class, so the request is handled as one
// without that header.
func (c *client) endpoint(ctx context.Context, classBaseURL string,
) (endpointURL, apiKey string, hostFromHeader bool, err error) {
	apiKey = modulecomponents.GetValueFromContext(ctx, apiKeyHeader)
	baseURL := classBaseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, baseURLHeader); headerBaseURL != "" {
		normalized, normalizeErr := config.NormalizeBaseURL(headerBaseURL)
		if normalizeErr != nil || normalized != classBaseURL {
			if apiKey == "" {
				return "", "", false, fmt.Errorf("%s needs %s: the server's key is not sent to a host named by the request",
					baseURLHeader, apiKeyHeader)
			}
			if normalizeErr != nil {
				return "", "", false, fmt.Errorf("%s header %w", baseURLHeader, normalizeErr)
			}
			if err := modulecomponents.ValidateBaseURL(normalized); err != nil {
				return "", "", false, err
			}
			baseURL = normalized
			hostFromHeader = true
		}
	}
	if apiKey == "" {
		apiKey = c.apiKey
	}
	if apiKey == "" {
		return "", "", false, fmt.Errorf("TypeSafe AI API key: no api key found neither in request header: %s "+
			"nor in environment variable under TYPESAFEAI_APIKEY", apiKeyHeader)
	}
	endpointURL, err = url.JoinPath(baseURL, apiPath)
	if err != nil {
		return "", "", false, fmt.Errorf("TypeSafe AI base URL %q: %w", baseURL, err)
	}
	return endpointURL, apiKey, hostFromHeader, nil
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

// newRequest asks about a single document with the query alone. Several
// documents become a map with one question per document, which names the
// document it is about. An image is an asset and the state holds its
// reference.
func newRequest(r judgeRequest, documents []ent.Document) (typesafeaiRequest, error) {
	var values []string
	var assets map[string]imageAsset
	if r.image {
		var err error
		assets, values, err = assetsOf(documents)
		if err != nil {
			return typesafeaiRequest{}, err
		}
	} else {
		values = make([]string, len(documents))
		for i, document := range documents {
			values[i] = document.Value
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
	// Usage is decoded apart, so that a usage of another shape does not fail
	// the answers.
	Usage json.RawMessage `json:"usage"`
}

type typesafeaiAnswer struct {
	Type string   `json:"type"`
	Noul *float64 `json:"noul"`
}

// typesafeaiUsage is what one or more requests cost: the input tokens the
// API reports, and the requests sent, including those rejected for their
// size.
type typesafeaiUsage struct {
	InputTokens int64
	requests    int64
}

// inputTokens returns the input tokens of a usage, truncated to a whole
// number, or 0 when the usage holds no number of them.
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

func (u typesafeaiUsage) add(other typesafeaiUsage) typesafeaiUsage {
	return typesafeaiUsage{InputTokens: u.InputTokens + other.InputTokens, requests: u.requests + other.requests}
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
		return 0, fmt.Errorf("TypeSafe AI answer for %q has type %q, want %q", key, truncate(answer.Type), noulType)
	}
	if answer.Noul == nil {
		return 0, fmt.Errorf("TypeSafe AI answer for %q has no probability", key)
	}
	if p := *answer.Noul; p < 0 || p > 1 {
		return 0, fmt.Errorf("TypeSafe AI answer for %q has probability %v, outside [0, 1]", key, p)
	}
	return *answer.Noul, nil
}
