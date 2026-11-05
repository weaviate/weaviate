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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// The API takes an image only as an inline data URL; hosted URLs and file ids
// are rejected.
const (
	inputRoleUser  = "user"
	inputImageType = "input_image"
)

type inputMessage struct {
	Role    string      `json:"role"`
	Content []inputPart `json:"content"`
}

type inputPart struct {
	Type     string `json:"type"`
	ImageURL string `json:"image_url,omitempty"`
}

// WebP is a RIFF container with "WEBP" at offset 8; a WAVE file shares the RIFF header.
var imageTypes = []struct {
	mime   string
	magics []magic
}{
	{"image/png", []magic{{0, []byte("\x89PNG")}}},
	{"image/jpeg", []magic{{0, []byte("\xff\xd8\xff")}}},
	{"image/gif", []magic{{0, []byte("GIF8")}}},
	{"image/webp", []magic{{0, []byte("RIFF")}, {8, []byte("WEBP")}}},
}

type magic struct {
	offset int
	bytes  []byte
}

func (m magic) matches(decoded []byte) bool {
	return len(decoded) >= m.offset+len(m.bytes) && bytes.Equal(decoded[m.offset:m.offset+len(m.bytes)], m.bytes)
}

// magicChars is the base64 of the first 12 bytes.
const magicChars = 16

// The API rejects images over maxImageBytes.
const maxImageBytes = 20 << 20

// imageMIME decodes only the head of the image.
func imageMIME(encoded string) (string, error) {
	// Each base64 character other than the padding carries 6 bits.
	if size := len(strings.TrimRight(encoded, "=")) * 6 / 8; size > maxImageBytes {
		return "", fmt.Errorf("%d bytes exceed the limit of %d MiB per image", size, maxImageBytes>>20)
	}
	head := encoded[:min(len(encoded), magicChars)]
	decoded, err := base64.StdEncoding.DecodeString(head)
	if err != nil {
		// A prefix cut inside a quad decodes once trimmed to whole quads.
		decoded, err = base64.StdEncoding.DecodeString(head[:len(head)/4*4])
		if err != nil {
			return "", errors.New("not base64")
		}
	}
	for _, format := range imageTypes {
		if slices.ContainsFunc(format.magics, func(m magic) bool { return !m.matches(decoded) }) {
			continue
		}
		return format.mime, nil
	}
	return "", errors.New("not a PNG, JPEG, GIF or WebP image")
}

func imageDataURL(encoded string) (string, error) {
	mime, err := imageMIME(encoded)
	if err != nil {
		return "", err
	}
	return "data:" + mime + ";base64," + encoded, nil
}

// checkImages runs before any request is sent and billed.
func checkImages(documents []ent.Document, positions []int) error {
	for _, position := range positions {
		if _, err := imageMIME(documents[position].Value); err != nil {
			return fmt.Errorf("object %s: image: %w", documents[position].ID, err)
		}
	}
	return nil
}

func newInput(document string, image bool) (any, error) {
	if !image {
		return document, nil
	}
	url, err := imageDataURL(document)
	if err != nil {
		return nil, err
	}
	return []inputMessage{{Role: inputRoleUser, Content: []inputPart{{Type: inputImageType, ImageURL: url}}}}, nil
}
