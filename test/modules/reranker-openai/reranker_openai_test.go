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

package test

import (
	"bytes"
	"context"
	"encoding/base64"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"net/http"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	className = "Tickets"
	churn     = "The customer is about to churn"
	darkCross = "The image contains a dark cross"

	changeEmail  = "How do I change the email on my account?"
	noRefund     = "Three weeks and still no refund. Nobody answers my emails."
	chargedTwice = "I was charged twice and the refund never came."

	changeEmailID  = "00000000-0000-0000-0000-000000000001"
	noRefundID     = "00000000-0000-0000-0000-000000000002"
	chargedTwiceID = "00000000-0000-0000-0000-000000000003"
	whiteID        = "00000000-0000-0000-0000-000000000004"
	crossID        = "00000000-0000-0000-0000-000000000005"
)

type ticket struct {
	id     string
	text   string
	photo  string
	vector []float32
}

// Tickets are in the order the search near {1, 0} returns them.
func tickets(photos ...string) []ticket {
	out := []ticket{
		{id: changeEmailID, text: changeEmail, vector: []float32{1, 0}},
		{id: noRefundID, text: noRefund, vector: []float32{1, 1}},
		{id: chargedTwiceID, text: chargedTwice, vector: []float32{0, 1}},
	}
	for i, photo := range photos {
		out[i].photo = photo
	}
	return out
}

func testRerankerOpenAI(restURI, grpcURI string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(restURI)
		helper.SetupGRPCClient(t, grpcURI)
		defer helper.ResetClient()
		grpcClient := helper.ClientGRPC(t)

		white := grayPNG(t, 400, 0xff)
		cross := crossPNG(t, 400)
		tests := []struct {
			name       string
			tickets    []ticket
			property   string
			condition  string
			wantLowest string
		}{
			{
				name:       "the email question scores lowest",
				tickets:    tickets(),
				property:   "text",
				condition:  churn,
				wantLowest: changeEmailID,
			},
			{
				name: "the cross scores above the plain image",
				tickets: []ticket{
					{id: whiteID, photo: white, vector: []float32{1, 0}},
					{id: crossID, photo: cross, vector: []float32{0, 1}},
				},
				property:   "photo",
				condition:  darkCross,
				wantLowest: whiteID,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				createTickets(t, map[string]any{}, tt.tickets)

				reply, err := grpcClient.Search(context.Background(), rerankRequest(tt.property, new(tt.condition)))

				require.NoError(t, err)
				require.Len(t, reply.Results, len(tt.tickets))
				ids, scores := resultIDs(reply.Results), rerankScores(reply.Results)
				t.Logf("rerank scores: %v of %v", scores, ids)
				for _, r := range reply.Results {
					assert.True(t, r.Metadata.RerankScorePresent)
					assert.GreaterOrEqual(t, r.Metadata.RerankScore, 0.0)
					assert.LessOrEqual(t, r.Metadata.RerankScore, 1.0)
				}
				assert.IsNonIncreasing(t, scores)
				last := len(scores) - 1
				assert.Equal(t, tt.wantLowest, ids[last])
				assert.Less(t, scores[last], scores[last-1])
			})
		}

		t.Run("a page of one is a request of one document", func(t *testing.T) {
			createTickets(t, map[string]any{}, tickets())
			request := rerankRequest("text", new(churn))
			request.Limit = 1

			reply, err := grpcClient.Search(context.Background(), request)

			require.NoError(t, err)
			require.Len(t, reply.Results, 1)
			metadata := reply.Results[0].Metadata
			t.Logf("rerank score: %v of %v", metadata.RerankScore, metadata.Id)
			assert.True(t, metadata.RerankScorePresent)
			assert.GreaterOrEqual(t, metadata.RerankScore, 0.0)
			assert.LessOrEqual(t, metadata.RerankScore, 1.0)
		})
	}
}

func testRerankerOpenAIAgainstAFakeProvider(fake *fakeProvider, restURI, grpcURI string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(restURI)
		helper.SetupGRPCClient(t, grpcURI)
		defer helper.ResetClient()
		grpcClient := helper.ClientGRPC(t)

		photos := []string{grayPNG(t, 300, 0x40), grayPNG(t, 300, 0x80), grayPNG(t, 300, 0xc0)}
		oversized := oversizedImage(t)
		allProbabilities := map[string]float64{changeEmail: 0.2, noRefund: 0.9, chargedTwice: 0.5}
		tests := []struct {
			name          string
			photos        []string
			probabilities map[string]float64
			refusals      map[string]bool
			failures      map[string]fakeReply
			property      string
			query         *string
			wantIDs       []string
			wantScores    []float64
			wantInputs    []any
			failing       string
			minRequests   int
			maxRequests   int
			wantErr       []string
			wantNotInErr  []string
		}{
			{
				name:          "the page is sorted by the probabilities",
				probabilities: allProbabilities,
				property:      "text",
				query:         new(churn),
				wantIDs:       []string{noRefundID, chargedTwiceID, changeEmailID},
				wantScores:    []float64{0.9, 0.5, 0.2},
				wantInputs:    []any{changeEmail, noRefund, chargedTwice},
				minRequests:   3,
				maxRequests:   3,
			},
			{
				name:          "an image request carries the photo as a data URL",
				photos:        photos,
				probabilities: map[string]float64{photos[0]: 0.2, photos[1]: 0.9, photos[2]: 0.5},
				property:      "photo",
				query:         new(churn),
				wantIDs:       []string{noRefundID, chargedTwiceID, changeEmailID},
				wantScores:    []float64{0.9, 0.5, 0.2},
				wantInputs:    []any{imageInput(photos[0]), imageInput(photos[1]), imageInput(photos[2])},
				minRequests:   3,
				maxRequests:   3,
			},
			{
				name:          "a 429 fails the query without retries",
				probabilities: allProbabilities,
				failing:       noRefund,
				failures:      map[string]fakeReply{noRefund: {status: http.StatusTooManyRequests, body: `{"error":{"message":"rate limited"}}`}},
				property:      "text",
				query:         new(churn),
				wantErr:       []string{"connection to OpenAI API failed with status 429: rate limited"},
				minRequests:   1,
				maxRequests:   3,
			},
			{
				name:          "a 500 fails the query without retries",
				probabilities: allProbabilities,
				failing:       noRefund,
				failures:      map[string]fakeReply{noRefund: {status: http.StatusInternalServerError, body: `{"error":{"message":"internal error"}}`}},
				property:      "text",
				query:         new(churn),
				wantErr:       []string{"connection to OpenAI API failed with status 500: internal error"},
				minRequests:   1,
				maxRequests:   3,
			},
			{
				name:          "a 401 does not quote the reply",
				probabilities: allProbabilities,
				failing:       noRefund,
				failures: map[string]fakeReply{noRefund: {
					status: http.StatusUnauthorized,
					body:   `{"error":{"message":"Incorrect API key provided: sk-fake-leaked-key"}}`,
				}},
				property:     "text",
				query:        new(churn),
				wantErr:      []string{"OpenAI rejected the API key (status 401)"},
				wantNotInErr: []string{"sk-fake-leaked-key", "Incorrect API key"},
				minRequests:  1,
				maxRequests:  3,
			},
			{
				name:          "a refused object scores 0 and the others keep their order",
				probabilities: allProbabilities,
				refusals:      map[string]bool{noRefund: true},
				property:      "text",
				query:         new(churn),
				wantIDs:       []string{chargedTwiceID, changeEmailID, noRefundID},
				wantScores:    []float64{0.5, 0.2, 0},
				wantInputs:    []any{changeEmail, noRefund, chargedTwice},
				minRequests:   3,
				maxRequests:   3,
			},
			{
				name:        "a missing query fails before any request",
				property:    "text",
				wantErr:     []string{"rerank.query is required by reranker-openai"},
				minRequests: 0,
				maxRequests: 0,
			},
			{
				name:        "an image above 20 MiB fails before any request",
				photos:      []string{oversized},
				property:    "photo",
				query:       new(churn),
				wantErr:     []string{changeEmailID, "exceed the limit of 20 MiB per image"},
				minRequests: 0,
				maxRequests: 0,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				fake.reset(tt.probabilities, tt.refusals, tt.failures)
				createTickets(t, map[string]any{"baseURL": fake.baseURL()}, tickets(tt.photos...))

				reply, err := grpcClient.Search(context.Background(), rerankRequest(tt.property, tt.query))

				requests := fake.received()
				if len(tt.wantErr) > 0 {
					require.Error(t, err)
					for _, part := range tt.wantErr {
						assert.ErrorContains(t, err, part)
					}
					for _, part := range tt.wantNotInErr {
						assert.NotContains(t, err.Error(), part)
					}
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.wantIDs, resultIDs(reply.Results))
					assert.Equal(t, tt.wantScores, rerankScores(reply.Results))
				}
				assert.GreaterOrEqual(t, len(requests), tt.minRequests)
				assert.LessOrEqual(t, len(requests), tt.maxRequests)
				inputs := make([]any, len(requests))
				sent := map[string]int{}
				for i, r := range requests {
					sent[documentOf(r.body.Input)]++
					assert.Equal(t, "/v1/decisions", r.path)
					assert.Equal(t, "Bearer "+fakeAPIKey, r.authorization)
					assert.Equal(t, fakeOrganization, r.organization)
					assert.Equal(t, "gpt-6-luna", r.body.Model)
					assert.Equal(t, []wireQuestion{{Type: "predicate", Name: "condition", Instructions: churn}}, r.body.Questions)
					inputs[i] = r.body.Input
				}
				if tt.failing != "" {
					assert.Equalf(t, 1, sent[tt.failing], "failing document %.40q sent %d times", tt.failing, sent[tt.failing])
				}
				for document, n := range sent {
					assert.Equalf(t, 1, n, "document %.40q sent %d times", document, n)
				}
				if tt.wantInputs != nil {
					assert.ElementsMatch(t, tt.wantInputs, inputs)
				}
			})
		}
	}
}

func imageInput(photo string) any {
	return []any{map[string]any{
		"role": "user",
		"content": []any{map[string]any{
			"type":      "input_image",
			"image_url": "data:image/png;base64," + photo,
		}},
	}}
}

// A PNG header followed by zeros, a few bytes above 20 MiB.
func oversizedImage(t *testing.T) string {
	t.Helper()
	content := make([]byte, 20<<20+8)
	copy(content, "\x89PNG\r\n\x1a\n")
	return base64.StdEncoding.EncodeToString(content)
}

func createTickets(t *testing.T, moduleConfig map[string]any, tickets []ticket) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class:        className,
		Vectorizer:   "none",
		ModuleConfig: map[string]any{"reranker-openai": moduleConfig},
		Properties: []*models.Property{
			{Name: "text", DataType: schema.DataTypeText.PropString()},
			{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
		},
	})
	t.Cleanup(func() { helper.DeleteClass(t, className) })
	for _, tk := range tickets {
		properties := map[string]any{}
		if tk.text != "" {
			properties["text"] = tk.text
		}
		if tk.photo != "" {
			properties["photo"] = tk.photo
		}
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      className,
			ID:         strfmt.UUID(tk.id),
			Vector:     tk.vector,
			Properties: properties,
		}))
	}
}

func rerankRequest(property string, query *string) *pb.SearchRequest {
	return &pb.SearchRequest{
		Collection: className,
		NearVector: &pb.NearVector{
			Vectors: []*pb.Vectors{{
				Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				VectorBytes: byteops.Fp32SliceToBytes([]float32{1, 0}),
			}},
		},
		Rerank:      &pb.Rerank{Property: property, Query: query},
		Metadata:    &pb.MetadataRequest{Uuid: true},
		Uses_127Api: true,
	}
}

func resultIDs(results []*pb.SearchResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.Metadata.Id
	}
	return ids
}

func rerankScores(results []*pb.SearchResult) []float64 {
	scores := make([]float64, len(results))
	for i, r := range results {
		scores[i] = r.Metadata.RerankScore
	}
	return scores
}

func grayPNG(t *testing.T, side int, level uint8) string {
	t.Helper()
	img := image.NewGray(image.Rect(0, 0, side, side))
	draw.Draw(img, img.Bounds(), image.NewUniform(color.Gray{Y: level}), image.Point{}, draw.Src)
	return encodePNG(t, img)
}

// A white square with a black cross, each bar a fifth of the side wide.
func crossPNG(t *testing.T, side int) string {
	t.Helper()
	img := image.NewGray(image.Rect(0, 0, side, side))
	draw.Draw(img, img.Bounds(), image.White, image.Point{}, draw.Src)
	bar := side / 5
	start := (side - bar) / 2
	draw.Draw(img, image.Rect(start, 0, start+bar, side), image.Black, image.Point{}, draw.Src)
	draw.Draw(img, image.Rect(0, start, side, start+bar), image.Black, image.Point{}, draw.Src)
	return encodePNG(t, img)
}

func encodePNG(t *testing.T, img image.Image) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}
