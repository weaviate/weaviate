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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

// A 1x1 PNG, and the first bytes of the other formats, as a blob property
// holds them.
const (
	pngBase64  = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGNgYGAAAAAEAAH2FzhVAAAAAElFTkSuQmCC"
	jpegBase64 = "/9j/4AAAAAAAAAAAAAAAAA=="
	gifBase64  = "R0lGODlhAAAAAAAAAAAAAA=="
	webpBase64 = "UklGRhAAAABXRUJQVlA4IAAAAAAAAAAA"
	textBase64 = "aGVsbG8gd29ybGQsIHRoaXMgaXMgdGV4dA=="
)

func TestImageDataURL(t *testing.T) {
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
		{name: "base64 of text", encoded: textBase64, wantErr: "not a PNG, JPEG, GIF or WebP image"},
		{name: "not base64", encoded: "%%%%", wantErr: "not base64"},
		{name: "empty", encoded: "", wantErr: "not a PNG, JPEG, GIF or WebP image"},
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

func TestIsImageProperty(t *testing.T) {
	cfg := classConfigWithTypes("", map[string]schema.DataType{"photo": schema.DataTypeBlob, "text": schema.DataTypeText})

	assert.True(t, isImageProperty(cfg, "photo"))
	assert.False(t, isImageProperty(cfg, "text"))
	assert.False(t, isImageProperty(cfg, "unknown"))
	assert.False(t, isImageProperty(cfg, ""))
	assert.False(t, isImageProperty(nil, "photo"))
}
