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
	"encoding/base64"
	"encoding/binary"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// encodedImage is a base64 image of the given size, as a blob property
// holds it.
func encodedImage(t *testing.T, format string, width, height int) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for x := 0; x < width; x += 7 {
		img.Set(x, 0, color.RGBA{R: 200, A: 255})
	}
	var buf bytes.Buffer
	switch format {
	case "png":
		require.NoError(t, png.Encode(&buf, img))
	case "jpeg":
		require.NoError(t, jpeg.Encode(&buf, img, &jpeg.Options{Quality: 50}))
	case "gif":
		require.NoError(t, gif.Encode(&buf, img, nil))
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

// animatedPNG is a 320x280 PNG with an acTL chunk inserted after IHDR, as
// an animated PNG carries it.
func animatedPNG(t *testing.T) string {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(encodedImage(t, "png", 320, 280))
	require.NoError(t, err)
	chunk := make([]byte, 20)
	binary.BigEndian.PutUint32(chunk[0:4], 8)
	copy(chunk[4:8], "acTL")
	// Signature (8) + IHDR chunk (25 bytes).
	at := 33
	out := append(append(append([]byte{}, decoded[:at]...), chunk...), decoded[at:]...)
	return base64.StdEncoding.EncodeToString(out)
}

func TestCheckImage(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
		wantErr string
	}{
		{name: "png of 280x280", encoded: encodedImage(t, "png", 280, 280)},
		{name: "jpeg of 1568x280", encoded: encodedImage(t, "jpeg", 1568, 280)},
		{name: "jpeg of 1568x1568", encoded: encodedImage(t, "jpeg", 1568, 1568)},
		{name: "jpeg of 300x1000", encoded: encodedImage(t, "jpeg", 300, 1000)},
		{name: "a side below 280", encoded: encodedImage(t, "png", 279, 400), wantErr: "279x400 pixels: the API takes 280 to 1568 pixels per side"},
		{name: "a side above 1568", encoded: encodedImage(t, "jpeg", 1569, 300), wantErr: "1569x300 pixels: the API takes 280 to 1568 pixels per side"},
		{name: "animated png", encoded: animatedPNG(t), wantErr: "animated PNG: the API takes still images"},
		{name: "gif", encoded: encodedImage(t, "gif", 320, 280), wantErr: "not a PNG or JPEG image"},
		{name: "base64 of text", encoded: base64.StdEncoding.EncodeToString([]byte("hello world, this is text")), wantErr: "not a PNG or JPEG image"},
		{name: "not base64", encoded: "!!!!", wantErr: "not base64"},
		{name: "above the size limit", encoded: encodedImage(t, "png", 320, 280) + strings.Repeat("A", maxImageChars), wantErr: "exceed the limit of 16 MB per image"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkImage(tt.encoded)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestImageFormat(t *testing.T) {
	png := encodedImage(t, "png", 320, 280)
	jpg := encodedImage(t, "jpeg", 320, 280)
	for name, tt := range map[string]struct{ encoded, want string }{
		"png":          {png, "png"},
		"jpeg":         {jpg, "jpeg"},
		"a png prefix": {png[:10], "png"},
	} {
		t.Run(name, func(t *testing.T) {
			format, err := imageFormat(tt.encoded)
			require.NoError(t, err)
			assert.Equal(t, tt.want, format)
		})
	}
}

func TestImageBatches(t *testing.T) {
	documents := make([]string, MaxBatchImages+2)
	indexes := make([]int, len(documents))
	for i := range documents {
		documents[i] = "x"
		indexes[i] = i
	}
	assert.Equal(t, [][]int{indexes[:MaxBatchImages], indexes[MaxBatchImages:]}, imageBatches(indexes, documents))

	// A large image goes alone.
	big := []string{"x", strings.Repeat("y", MaxBatchImageChars), "z"}
	assert.Equal(t, [][]int{{0}, {1}, {2}}, imageBatches([]int{0, 1, 2}, big))
}

// classConfigWithTypes is a class config that knows the data type of its
// properties, which decides whether a document is sent as an image.
func classConfigWithTypes(baseURL string, settings map[string]any, dataTypes map[string]schema.DataType) moduletools.ClassConfig {
	cfg := classConfig(baseURL, settings).(fakeClassConfig)
	cfg.dataTypes = dataTypes
	return cfg
}

func TestRankImages(t *testing.T) {
	photo := encodedImage(t, "jpeg", 320, 280)
	other := encodedImage(t, "png", 400, 300)
	blobs := map[string]schema.DataType{"photo": schema.DataTypeBlob, "text": schema.DataTypeText}

	t.Run("sends the images as assets with the header", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{imageReference(0): 0.9, imageReference(1): 0.2}}
		server := httptest.NewServer(handler)
		defer server.Close()

		res, err := newTestClient("apiKey").RankProperty(context.Background(), "the car is damaged", "photo", true,
			[]string{photo, other}, classConfigWithTypes(server.URL, map[string]any{"images": true}, blobs))

		require.NoError(t, err)
		assert.Equal(t, []ent.DocumentScore{{Document: photo, Score: 0.9}, {Document: other, Score: 0.2}}, res.DocumentScores)
		requests := handler.received()
		require.Len(t, requests, 1)
		req := requests[0]
		assert.Equal(t, "true", req.header, "the danger-zone header switches image input on")
		state, ok := req.body.State.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, map[string]any{batchKey(0): imageReference(0), batchKey(1): imageReference(1)}, state)
		require.Len(t, req.body.Assets, 2)
		assert.Equal(t, imageAsset{Type: "image", DataBase64: photo, Format: "jpeg"}, req.body.Assets[imageReference(0)])
		assert.Equal(t, imageAsset{Type: "image", DataBase64: other, Format: "png"}, req.body.Assets[imageReference(1)])
		assert.Equal(t, "In "+batchKey(0)+": the car is damaged", req.body.Questions[batchKey(0)].Instructions)
	})

	t.Run("a single image is the state", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{imageReference(0): 0.7}}
		server := httptest.NewServer(handler)
		defer server.Close()

		res, err := newTestClient("apiKey").RankProperty(context.Background(), "q", "photo", false,
			[]string{photo}, classConfigWithTypes(server.URL, map[string]any{"images": true}, blobs))

		require.NoError(t, err)
		assert.InDelta(t, 0.7, res.DocumentScores[0].Score, 1e-9)
		req := handler.received()[0]
		assert.Equal(t, imageReference(0), req.body.State)
		assert.Len(t, req.body.Assets, 1)
	})

	t.Run("a text property is sent as text without the header", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{"a document": 0.5}}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").RankProperty(context.Background(), "q", "text", false,
			[]string{"a document"}, classConfigWithTypes(server.URL, map[string]any{"images": true}, blobs))

		require.NoError(t, err)
		req := handler.received()[0]
		assert.Equal(t, "", req.header)
		assert.Empty(t, req.body.Assets)
	})

	t.Run("a blob without the images setting is refused", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").RankProperty(context.Background(), "q", "photo", false,
			[]string{photo}, classConfigWithTypes(server.URL, nil, blobs))

		require.EqualError(t, err, `property "photo" is a blob: images need the images setting of decisions-typesafeai`)
		assert.Empty(t, handler.received())
	})

	t.Run("an image the API would reject is refused before any request", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").RankProperty(context.Background(), "q", "photo", false,
			[]string{photo, encodedImage(t, "png", 100, 100)}, classConfigWithTypes(server.URL, map[string]any{"images": true}, blobs))

		require.EqualError(t, err, "document 1: image: 100x100 pixels: the API takes 280 to 1568 pixels per side")
		assert.Empty(t, handler.received(), "the other images are not sent either")
	})
}

func TestDecideImages(t *testing.T) {
	photo := encodedImage(t, "jpeg", 320, 280)
	blobs := map[string]schema.DataType{"photo": schema.DataTypeBlob, "text": schema.DataTypeText}
	damaged := ent.DecisionQuestion{Name: "damaged", Property: "photo", Instructions: "the car is damaged", Kind: ent.DecisionPredicate}

	t.Run("sends the image as an asset with the header", func(t *testing.T) {
		handler := &decideHandler{t: t, scores: map[string]float64{imageReference(0): 0.8}}
		server := httptest.NewServer(handler)
		defer server.Close()

		answers, err := newTestClient("apiKey").Decide(context.Background(), []ent.DecisionQuestion{damaged},
			[]string{photo}, classConfigWithTypes(server.URL, map[string]any{"images": true}, blobs))

		require.NoError(t, err)
		require.Len(t, answers, 1)
		assert.InDelta(t, 0.8, answers[0][0].Probability, 1e-9)
		req := handler.received()[0]
		assert.Equal(t, imageReference(0), req.State)
		assert.Equal(t, imageAsset{Type: "image", DataBase64: photo, Format: "jpeg"}, req.Assets[imageReference(0)])
		assert.Equal(t, "true", handler.headers()[0])
	})

	t.Run("questions about two properties in one call are refused", func(t *testing.T) {
		handler := &decideHandler{t: t}
		server := httptest.NewServer(handler)
		defer server.Close()
		text := ent.DecisionQuestion{Name: "angry", Property: "text", Instructions: "angry", Kind: ent.DecisionPredicate}

		_, err := newTestClient("apiKey").Decide(context.Background(), []ent.DecisionQuestion{damaged, text},
			[]string{photo}, classConfigWithTypes(server.URL, map[string]any{"images": true}, blobs))

		require.EqualError(t, err, `questions about "photo" and "text" in one call: a call judges one property`)
		assert.Empty(t, handler.received())
	})

	t.Run("a blob without the images setting is refused", func(t *testing.T) {
		handler := &decideHandler{t: t}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Decide(context.Background(), []ent.DecisionQuestion{damaged},
			[]string{photo}, classConfigWithTypes(server.URL, nil, blobs))

		require.EqualError(t, err, `property "photo" is a blob: images need the images setting of decisions-typesafeai`)
		assert.Empty(t, handler.received())
	})
}
