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

	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// The Decisions API takes a document as text, or as the content parts of a
// user message. An image must be an inline data URL; hosted URLs and file
// ids are rejected.
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

// imageTypes are the image formats the API accepts, by the bytes a file of
// that format starts with. WebP has "WEBP" at offset 8 behind a RIFF header,
// which a WAVE file shares.
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

// magicChars is how many base64 characters hold the bytes that decide the
// format (12 bytes).
const magicChars = 16

// maxImageBytes bounds the image one request carries: the API rejects
// larger ones, and the data URL and the JSON body copy the whole content.
// maxImageChars is the same bound on the base64; it also admits the two
// bytes that round up to its last quad.
const (
	maxImageBytes = 20 << 20
	maxImageChars = (maxImageBytes + 2) / 3 * 4
)

// imageMIME reads the MIME type from the first bytes, without decoding the
// whole image.
func imageMIME(encoded string) (string, error) {
	if len(encoded) > maxImageChars {
		return "", fmt.Errorf("%d bytes exceed the limit of %d MiB per image", len(encoded)/4*3, maxImageBytes>>20)
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

// imageDataURL turns the base64 content of a blob property into the data
// URL the API takes.
func imageDataURL(encoded string) (string, error) {
	mime, err := imageMIME(encoded)
	if err != nil {
		return "", err
	}
	return "data:" + mime + ";base64," + encoded, nil
}

// checkImages fails on the first document at the given positions that is
// not an image the API takes, before any request is sent and billed.
func checkImages(documents []ent.Document, positions []int) error {
	for _, position := range positions {
		if _, err := imageMIME(documents[position].Value); err != nil {
			return fmt.Errorf("object %s: image: %w", documents[position].ID, err)
		}
	}
	return nil
}

// newInput is the input of a request: the document as text, or as an image
// part when it comes from a blob property.
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
