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
	"fmt"
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

// tickets returns the three tickets in the order the search near {1, 0}
// returns them, with the photos given, if any.
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

func testRerankerTypeSafeAI(restURI, grpcURI string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(restURI)
		helper.SetupGRPCClient(t, grpcURI)
		defer helper.ResetClient()
		grpcClient := helper.ClientGRPC(t)

		white := grayPNG(t, 400, 0xff)
		cross := crossPNG(t, 400)
		tests := []struct {
			name      string
			tickets   []ticket
			property  string
			condition string
			// wantLowest is the ticket that scores below every other one.
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
	}
}

func testRerankerTypeSafeAIAgainstAFakeProvider(fake *fakeProvider, restURI, grpcURI string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(restURI)
		helper.SetupGRPCClient(t, grpcURI)
		defer helper.ResetClient()
		grpcClient := helper.ClientGRPC(t)

		photos := []string{grayPNG(t, 300, 0x40), grayPNG(t, 300, 0x80), grayPNG(t, 300, 0xc0)}
		smallPhotos := []string{grayPNG(t, 100, 0x40), grayPNG(t, 100, 0x80), grayPNG(t, 100, 0xc0)}
		tests := []struct {
			name             string
			photos           []string
			probabilities    map[string]float64
			replies          map[int]fakeReply
			property         string
			query            *string
			wantIDs          []string
			wantScores       []float64
			wantRequest      *wireRequest
			wantImagesHeader string
			wantErr          []string
			wantRequests     int
		}{
			{
				name:          "the page is sorted by the probabilities",
				probabilities: map[string]float64{changeEmail: 0.2, noRefund: 0.9, chargedTwice: 0.5},
				property:      "text",
				query:         new(churn),
				wantIDs:       []string{noRefundID, chargedTwiceID, changeEmailID},
				wantScores:    []float64{0.9, 0.5, 0.2},
				wantRequest:   documentsRequest([]string{changeEmail, noRefund, chargedTwice}, false),
				wantRequests:  1,
			},
			{
				name:             "an image request carries the header and the assets",
				photos:           photos,
				probabilities:    map[string]float64{photos[0]: 0.2, photos[1]: 0.9, photos[2]: 0.5},
				property:         "photo",
				query:            new(churn),
				wantIDs:          []string{noRefundID, chargedTwiceID, changeEmailID},
				wantScores:       []float64{0.9, 0.5, 0.2},
				wantRequest:      documentsRequest(photos, true),
				wantImagesHeader: "true",
				wantRequests:     1,
			},
			{
				name:         "a 429 fails the query at once",
				replies:      map[int]fakeReply{0: {status: http.StatusTooManyRequests, body: `{"error":"rate limited"}`}},
				property:     "text",
				query:        new(churn),
				wantErr:      []string{"status 429", "rate limited"},
				wantRequests: 1,
			},
			{
				name:         "a 500 fails the query at once",
				replies:      map[int]fakeReply{0: {status: http.StatusInternalServerError, body: `{"error":"internal error"}`}},
				property:     "text",
				query:        new(churn),
				wantErr:      []string{"status 500"},
				wantRequests: 1,
			},
			{
				name:         "a missing query fails before any request",
				property:     "text",
				wantErr:      []string{"rerank.query is required by reranker-typesafeai"},
				wantRequests: 0,
			},
			{
				name:         "an image outside the limits fails before any request",
				photos:       smallPhotos,
				property:     "photo",
				query:        new(churn),
				wantErr:      []string{"280 to 1568 pixels per side"},
				wantRequests: 0,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				fake.reset(tt.probabilities, tt.replies)
				createTickets(t, map[string]any{"baseURL": fake.baseURL()}, tickets(tt.photos...))

				reply, err := grpcClient.Search(context.Background(), rerankRequest(tt.property, tt.query))

				requests := fake.received()
				if len(tt.wantErr) > 0 {
					require.Error(t, err)
					for _, part := range tt.wantErr {
						assert.ErrorContains(t, err, part)
					}
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.wantIDs, resultIDs(reply.Results))
					assert.Equal(t, tt.wantScores, rerankScores(reply.Results))
				}
				require.Len(t, requests, tt.wantRequests)
				if tt.wantRequest != nil {
					r := requests[0]
					assert.Equal(t, "/v1/systemone", r.path)
					assert.Equal(t, "Bearer "+fakeAPIKey, r.authorization)
					assert.Equal(t, tt.wantImagesHeader, r.imagesHeader)
					assert.Equal(t, *tt.wantRequest, r.body)
				}
			})
		}
	}
}

// documentsRequest is the request for the documents of the three tickets in
// the order of the search: their texts, or the base64 of their photos.
func documentsRequest(documents []string, images bool) *wireRequest {
	r := &wireRequest{Model: "jev-latest", Questions: map[string]wireQuestion{}}
	state := map[string]any{}
	if images {
		r.Assets = map[string]wireAsset{}
	}
	for i, document := range documents {
		key := fmt.Sprintf("document_%d", i)
		r.Questions[key] = wireQuestion{Type: "noul", Instructions: "In " + key + ": " + churn}
		if !images {
			state[key] = document
			continue
		}
		reference := fmt.Sprintf("image_%d", i)
		state[key] = reference
		r.Assets[reference] = wireAsset{Type: "image", DataBase64: document, Format: "png"}
	}
	r.State = state
	return r
}

func createTickets(t *testing.T, moduleConfig map[string]any, tickets []ticket) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class:        className,
		Vectorizer:   "none",
		ModuleConfig: map[string]any{"reranker-typesafeai": moduleConfig},
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

// grayPNG returns the base64 of a side by side PNG of one gray level.
func grayPNG(t *testing.T, side int, level uint8) string {
	t.Helper()
	img := image.NewGray(image.Rect(0, 0, side, side))
	draw.Draw(img, img.Bounds(), image.NewUniform(color.Gray{Y: level}), image.Point{}, draw.Src)
	return encodePNG(t, img)
}

// crossPNG returns the base64 of a white side by side PNG with a black cross
// through its middle, each bar a fifth of the side wide.
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
