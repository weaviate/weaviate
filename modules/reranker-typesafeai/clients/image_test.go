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
	"encoding/base64"
	"encoding/binary"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckImage(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
		wantErr string
	}{
		{name: "300x300 PNG", encoded: encodedImage(t, "png", 300, 300)},
		{name: "300x300 JPEG", encoded: encodedImage(t, "jpeg", 300, 300)},
		{name: "JPEG with an EXIF segment before the frame header", encoded: jpegWithEXIF(t)},
		{
			name:    "279x300",
			encoded: encodedImage(t, "png", 279, 300),
			wantErr: "279x300 pixels: the API takes 280 to 1568 pixels per side",
		},
		{
			name:    "1569x300",
			encoded: encodedImage(t, "jpeg", 1569, 300),
			wantErr: "1569x300 pixels: the API takes 280 to 1568 pixels per side",
		},
		{name: "animated PNG", encoded: animatedPNG(t), wantErr: "animated PNG: the API takes still images"},
		{name: "GIF", encoded: encodedImage(t, "gif", 300, 300), wantErr: "not a PNG or JPEG image"},
		{name: "not base64", encoded: "!!!!", wantErr: "not base64"},
		{name: "a PNG that turns into something else", encoded: encodedImage(t, "png", 300, 300) + "!!!!", wantErr: "not base64"},
		{name: "a PNG signature without IHDR", encoded: base64.StdEncoding.EncodeToString([]byte("\x89PNG\r\n\x1a\nshort")), wantErr: "PNG without an IHDR chunk"},
		{name: "a JPEG without a frame header", encoded: base64.StdEncoding.EncodeToString([]byte("\xff\xd8\xff\xda\x00\x02")), wantErr: "JPEG without a frame header"},
		{
			name:    "above 16 MiB, rejected by its length",
			encoded: strings.Repeat("A", maxImageChars+4),
			wantErr: "16777221 bytes exceed the limit of 16 MiB per image",
		},
		{
			name:    "one byte above 16 MiB",
			encoded: pngOfSize(t, maxImageBytes+1),
			wantErr: "16777217 bytes exceed the limit of 16 MiB per image",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkImage(tt.encoded)

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestAssetsOfNamesTheObjectOfAnImageItCannotRead(t *testing.T) {
	_, _, err := assetsOf(documentsOf(encodedImage(t, "png", 300, 300), "!!!!"))

	require.EqualError(t, err, "object 00000000-0000-0000-0000-000000000001: image: not base64")
}

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
	default:
		t.Fatalf("unknown format %q", format)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func decodedImage(t *testing.T, format string, width, height int) []byte {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(encodedImage(t, format, width, height))
	require.NoError(t, err)
	return decoded
}

// animatedPNG inserts an acTL chunk after IHDR, where an animated PNG carries
// it.
func animatedPNG(t *testing.T) string {
	t.Helper()
	decoded := decodedImage(t, "png", 300, 300)
	acTL := make([]byte, 20)
	binary.BigEndian.PutUint32(acTL[0:4], 8)
	copy(acTL[4:8], "acTL")
	const afterIHDR = 33
	animated := append(append(append([]byte{}, decoded[:afterIHDR]...), acTL...), decoded[afterIHDR:]...)
	return base64.StdEncoding.EncodeToString(animated)
}

// jpegWithEXIF puts an APP1 segment, where cameras store EXIF data, between
// the start of the image and the frame header.
func jpegWithEXIF(t *testing.T) string {
	t.Helper()
	decoded := decodedImage(t, "jpeg", 300, 300)
	app1 := append([]byte{0xff, 0xe1, 0x00, 0x10}, "Exif\x00\x00padding!"...)
	withEXIF := append(append(append([]byte{}, decoded[:2]...), app1...), decoded[2:]...)
	return base64.StdEncoding.EncodeToString(withEXIF)
}

// pngOfSize pads a valid 300x300 PNG after its end to size bytes.
func pngOfSize(t *testing.T, size int) string {
	t.Helper()
	decoded := decodedImage(t, "png", 300, 300)
	padded := make([]byte, size)
	copy(padded, decoded)
	return base64.StdEncoding.EncodeToString(padded)
}
