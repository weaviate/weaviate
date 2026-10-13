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

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
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
// that format starts with. WebP has "WEBP" at offset 8 behind a RIFF header.
var imageTypes = []struct {
	mime   string
	offset int
	magic  []byte
}{
	{"image/png", 0, []byte("\x89PNG")},
	{"image/jpeg", 0, []byte("\xff\xd8\xff")},
	{"image/gif", 0, []byte("GIF8")},
	{"image/webp", 8, []byte("WEBP")},
}

// magicBytes is how many decoded bytes decide the format, and magicChars
// the base64 characters that hold them.
const (
	magicBytes = 12
	magicChars = 16
)

// imageDataURL turns the base64 content of a blob property into the data
// URL the API takes, with the MIME type read from the first bytes.
func imageDataURL(encoded string) (string, error) {
	head := encoded
	if len(head) > magicChars {
		head = head[:magicChars]
	}
	decoded, err := base64.StdEncoding.DecodeString(head)
	if err != nil {
		// A prefix cut at a non-multiple of four still decodes its whole
		// quads; anything else is not base64.
		decoded, err = base64.StdEncoding.DecodeString(head[:len(head)/4*4])
		if err != nil {
			return "", errors.New("not base64")
		}
	}
	for _, format := range imageTypes {
		if len(decoded) >= format.offset+len(format.magic) &&
			bytes.Equal(decoded[format.offset:format.offset+len(format.magic)], format.magic) {
			return "data:" + format.mime + ";base64," + encoded, nil
		}
	}
	return "", errors.New("not a PNG, JPEG, GIF or WebP image")
}

// isImageProperty says whether the property is a blob, whose content the
// module sends as an image. An unknown property is text.
func isImageProperty(cfg moduletools.ClassConfig, property string) bool {
	if cfg == nil || property == "" {
		return false
	}
	return cfg.PropertiesDataTypes()[property] == schema.DataTypeBlob
}

// newInput is the input of a request: the document as text, or as an image
// part when it comes from a blob property.
func newInput(document string, image bool) (any, error) {
	if !image {
		return document, nil
	}
	url, err := imageDataURL(document)
	if err != nil {
		return nil, errors.Wrap(err, "image")
	}
	return []inputMessage{{Role: inputRoleUser, Content: []inputPart{{Type: inputImageType, ImageURL: url}}}}, nil
}
