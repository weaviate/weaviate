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
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A 1x1 PNG and the first bytes of the other formats.
const (
	pngBase64  = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGNgYGAAAAAEAAH2FzhVAAAAAElFTkSuQmCC"
	jpegBase64 = "/9j/4AAAAAAAAAAAAAAAAA=="
	gifBase64  = "R0lGODlhAAAAAAAAAAAAAA=="
	webpBase64 = "UklGRhAAAABXRUJQVlA4IAAAAAAAAAAA"
	bmpBase64  = "Qk02AAAAAAAAADYAAAAoAAAA"
	textBase64 = "aGVsbG8gd29ybGQsIHRoaXMgaXMgdGV4dA=="
)

func TestImageDataURL(t *testing.T) {
	limitPNG := pngOfSize(maxImageBytes)
	tests := []struct {
		name    string
		encoded string
		want    string
		wantErr string
	}{
		{name: "png", encoded: pngBase64, want: "data:image/png;base64," + pngBase64},
		{name: "jpeg", encoded: jpegBase64, want: "data:image/jpeg;base64," + jpegBase64},
		{name: "gif", encoded: gifBase64, want: "data:image/gif;base64," + gifBase64},
		{name: "webp", encoded: webpBase64, want: "data:image/webp;base64," + webpBase64},
		{name: "a short png prefix still names the format", encoded: pngBase64[:8], want: "data:image/png;base64," + pngBase64[:8]},
		{name: "a RIFF file that is not WebP", encoded: "UklGRhAAAABXQVZFZm10IAAA", wantErr: "not a PNG, JPEG, GIF or WebP image"},
		{name: "WEBP at offset 8 without RIFF", encoded: "SlVOSxAAAABXRUJQVlA4IAAA", wantErr: "not a PNG, JPEG, GIF or WebP image"},
		{name: "bmp", encoded: bmpBase64, wantErr: "not a PNG, JPEG, GIF or WebP image"},
		{name: "base64 of text", encoded: textBase64, wantErr: "not a PNG, JPEG, GIF or WebP image"},
		{name: "not base64", encoded: "%%%%", wantErr: "not base64"},
		{name: "empty", encoded: "", wantErr: "not a PNG, JPEG, GIF or WebP image"},
		{
			name:    "exactly 20 MiB, one padding character",
			encoded: limitPNG,
			want:    "data:image/png;base64," + limitPNG,
		},
		{
			name:    "20 MiB + 1 byte, no padding, the same length as 20 MiB",
			encoded: pngOfSize(maxImageBytes + 1),
			wantErr: "20971521 bytes exceed the limit of 20 MiB per image",
		},
		{
			name:    "20 MiB + 2 bytes, two padding characters",
			encoded: pngOfSize(maxImageBytes + 2),
			wantErr: "20971522 bytes exceed the limit of 20 MiB per image",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := imageDataURL(tt.encoded)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCheckImages(t *testing.T) {
	documents := documentsOf(pngBase64, textBase64, "", jpegBase64)

	assert.NoError(t, checkImages(documents, []int{0, 3}))
	assert.EqualError(t, checkImages(documents, []int{0, 1, 3}),
		"object 00000000-0000-0000-0000-000000000001: image: not a PNG, JPEG, GIF or WebP image")
}

func pngOfSize(size int) string {
	raw := make([]byte, size)
	copy(raw, "\x89PNG")
	return base64.StdEncoding.EncodeToString(raw)
}

func TestImageSizeLimitCases(t *testing.T) {
	limit, overByOne, overByTwo := pngOfSize(maxImageBytes), pngOfSize(maxImageBytes+1), pngOfSize(maxImageBytes+2)

	assert.Len(t, overByOne, len(limit))
	assert.True(t, strings.HasSuffix(limit, "=") && !strings.HasSuffix(limit, "=="))
	assert.False(t, strings.HasSuffix(overByOne, "="))
	assert.True(t, strings.HasSuffix(overByTwo, "=="))
}
