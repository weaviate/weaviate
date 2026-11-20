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
	// maxConcurrentRequests is 16 requests in flight per query.
	maxConcurrentRequests = 16
	// maxConcurrentImageRequests is 4 requests in flight per query when the
	// documents are images: a request holds two to four copies of up to 24 MiB
	// of base64 while it is in flight.
	maxConcurrentImageRequests = 4

	// The API does not publish its request size limit: 141,000 characters
	// were accepted and 149,000 rejected. A batch closes before the bytes of
	// its documents plus their questions, each of which repeats the query,
	// would pass maxBatchCharacters. One document above the limit is sent
	// alone. A non-ASCII character takes several bytes, so a batch of such
	// text holds fewer characters than the limit. Beyond 50 documents a
	// request saves almost no tokens and answers more slowly than two sent in
	// parallel.
	//
	// maxBatchDocuments is 50 documents per request.
	maxBatchDocuments = 50
	// maxBatchCharacters is 100,000 bytes of documents and questions per
	// request.
	maxBatchCharacters = 100_000

	apiPath       = "/v1/systemone"
	questionKey   = "q"
	noulType      = "noul"
	apiKeyHeader  = "X-Typesafeai-Api-Key"
	baseURLHeader = "X-Typesafeai-Baseurl"
	// tooLargeError is the error_type of the 400 the API answers to a request
	// above its token limit.
	tooLargeError = "max_tokens_exceeded"
	// maxErrorBodyBytes is 512 bytes: the most of a reply body an error quotes.
	maxErrorBodyBytes = 512
	// maxResponseBodyBytes is 1 MiB (1,048,576 bytes): the largest reply body
	// the client reads.
	maxResponseBodyBytes = 1 << 20
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

// validateQuery returns the error of a rerank.query the module does not rank
// with: a blank one, and one whose question for a document does not fit in a
// request. The gRPC search rejects a blank query before the search; GraphQL
// reaches this check only. The API answers 400 to an empty query and scores
// the documents against a query of whitespace.
func (c *client) validateQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return errors.New("rerank.query is required by reranker-typesafeai: the query is the condition the model judges")
	}
	if c.questionBytes(query) >= maxBatchCharacters {
		return fmt.Errorf("rerank.query is %d bytes; reranker-typesafeai repeats it for every document within %d bytes per request",
			len(query), maxBatchCharacters)
	}
	return nil
}

// questionBytes is the size of the longest question a request repeats the
// query in.
func (c *client) questionBytes(query string) int {
	return len("In "+documentKey(maxBatchDocuments-1)+": ") + len(query)
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
	endpointURL, apiKey, hostFromHeader, err := c.endpoint(ctx, settings.BaseURL())
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

	maxDocuments, maxCharacters, maxInFlight := maxBatchDocuments, maxBatchCharacters, maxConcurrentRequests
	if image {
		maxDocuments, maxCharacters, maxInFlight = maxBatchImages, maxBatchImageChars, maxConcurrentImageRequests
	}
	request := judgeRequest{
		url: endpointURL, apiKey: apiKey, hostFromHeader: hostFromHeader,
		model: settings.Model(), query: query, image: image,
	}
	requestBatches := c.batches(pending, documents, maxDocuments, maxCharacters, c.questionBytes(query))
	scores := make([]float64, len(documents))
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(maxInFlight)
	for _, batch := range requestBatches {
		eg.Go(func() error {
			// After a failed batch, the rest would still build their bodies, up
			// to 24 MiB of base64 each for images.
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
// maxDocuments documents and maxCharacters bytes, in order. A document costs
// its bytes plus questionBytes. A document that costs more than maxCharacters
// goes alone.
func (c *client) batches(indexes []int, documents []string, maxDocuments, maxCharacters, questionBytes int) [][]int {
	var out [][]int
	var batch []int
	characters := 0
	for _, index := range indexes {
		length := len(documents[index]) + questionBytes
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
	values, err := response.values(len(documents))
	if err != nil {
		return nil, c.replyError(err, nil, r.hostFromHeader)
	}
	return values, nil
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
	return c.send(ctx, r.url, r.apiKey, body, r.image, r.hostFromHeader)
}

// send does not retry a status: a 429 or a 5xx fails the call. The transport
// sends the request again, up to twice, when a reused connection breaks before
// a response arrives, so the API can receive a request three times. tooLarge
// says the API
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
		return typesafeaiResponse{}, false, c.transportError(ctx, "send TypeSafe AI request",
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
		return typesafeaiResponse{}, false, c.transportError(ctx, "read TypeSafe AI response",
			"incomplete response from the host named by "+baseURLHeader, err, hostFromHeader)
	}
	if len(responseBody) > maxResponseBodyBytes {
		return typesafeaiResponse{}, false, c.replyError(
			fmt.Errorf("TypeSafe AI response exceeds %d bytes", maxResponseBodyBytes), nil, hostFromHeader)
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		// Value holds the number literal of the reply when it is out of range,
		// and Field the keys of its answers.
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			typeErr.Value, _, _ = strings.Cut(typeErr.Value, " ")
			typeErr.Struct, typeErr.Field = "", ""
		}
		return typesafeaiResponse{}, false, c.replyError(fmt.Errorf("parse TypeSafe AI response: %s", truncate(err.Error())),
			responseBody, hostFromHeader)
	}
	// The shape of an error in a 200 is not documented, so the body is the
	// provider's message.
	if len(response.Answers) == 0 {
		return typesafeaiResponse{}, false, c.replyError(errors.New("TypeSafe AI response has no answers"),
			responseBody, hostFromHeader)
	}
	return response, false, nil
}

// transportError wraps err for the class or default host. For a host the
// request header named it returns headerHostMessage instead: net/http can
// quote bytes the host sent, and a dial or read error names the address it
// resolved to. An error of the context or of the client timeout stays
// identifiable with errors.Is.
func (c *client) transportError(ctx context.Context, action, headerHostMessage string, err error, hostFromHeader bool) error {
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

// replyError is the error of a 200 reply the client cannot use. For the class
// or default host it is err with the start of the body. For a host the request
// header named it is a fixed message: err can quote the reply, such as the
// type or the probability of an answer.
func (c *client) replyError(err error, body []byte, hostFromHeader bool) error {
	if hostFromHeader {
		return errors.New("unexpected TypeSafe AI response from the host named by " + baseURLHeader)
	}
	if len(body) == 0 {
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
// a host the request names.
func (c *client) endpoint(ctx context.Context, classBaseURL string,
) (endpointURL, apiKey string, hostFromHeader bool, err error) {
	apiKey = modulecomponents.GetValueFromContext(ctx, apiKeyHeader)
	baseURL := classBaseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, baseURLHeader); headerBaseURL != "" {
		if apiKey == "" {
			return "", "", false, fmt.Errorf("%s needs %s: the server's key is not sent to a host named by the request",
				baseURLHeader, apiKeyHeader)
		}
		if err := modulecomponents.ValidateBaseURL(headerBaseURL); err != nil {
			return "", "", false, err
		}
		baseURL, hostFromHeader = headerBaseURL, true
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
