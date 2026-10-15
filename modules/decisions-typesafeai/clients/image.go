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
	"strconv"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

// The API takes images as assets of a request: the base64 in the body
// under a reference, and the reference wherever the image belongs in the
// state. Image input is an experimental feature, off by default, that
// every request with an image switches on with a header. The limits are
// the API's, verified against it on 2026-10-15: PNG or JPEG, still images,
// 16 MiB per image, 280 to 1568 pixels per side (the documentation says
// 256; the API rejects 279). An image counts as input tokens like text,
// about one token per 375 pixels plus 270 (measured: 280x280 is 476,
// 1568x1568 is 6,548), and the state of a batch counts against the 32k
// tokens one question may see, so a request carries a few images only.
const (
	dangerZoneImagesHeader = "TypeSafe-Danger-Zone-Images"
	assetTypeImage         = "image"
	formatPNG              = "png"
	formatJPEG             = "jpeg"

	maxImageBytes = 16 << 20
	maxImageChars = maxImageBytes / 3 * 4
	minImageSide  = 280
	maxImageSide  = 1568

	// MaxBatchImages and MaxBatchImageChars bound one request with images.
	// Four images of the largest size are about 26k tokens, under the 32k
	// a question may see; the API accepts 32 MiB per request body, and the
	// base64 of 24 MiB leaves room for the JSON. A request the API still
	// rejects for its size is sent as halves, as for text.
	MaxBatchImages     = 4
	MaxBatchImageChars = 24 << 20
)

// imageAsset is one entry of the assets map of a request.
type imageAsset struct {
	Type       string `json:"type"`
	DataBase64 string `json:"data_base64"`
	Format     string `json:"format"`
}

// imageReference names the i-th image of a request in its state.
func imageReference(i int) string {
	return "image_" + strconv.Itoa(i)
}

var (
	pngMagic  = []byte("\x89PNG")
	jpegMagic = []byte("\xff\xd8\xff")
)

// imageFormat reads the format of a base64 image from its first bytes.
func imageFormat(encoded string) (string, error) {
	head := encoded
	if len(head) > 16 {
		head = head[:16]
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
	switch {
	case bytes.HasPrefix(decoded, pngMagic):
		return formatPNG, nil
	case bytes.HasPrefix(decoded, jpegMagic):
		return formatJPEG, nil
	}
	return "", errors.New("not a PNG or JPEG image")
}

// checkImage fails when the API would reject the image: the format, the
// size, an animated PNG, or the dimensions. It decodes the whole content,
// which the request copies anyway.
func checkImage(encoded string) error {
	if len(encoded) > maxImageChars {
		return errors.Errorf("%d MB exceed the limit of %d MB per image", len(encoded)/4*3>>20, maxImageBytes>>20)
	}
	format, err := imageFormat(encoded)
	if err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return errors.New("not standard base64 with padding")
	}
	var width, height int
	if format == formatPNG {
		width, height, err = pngSize(decoded)
	} else {
		width, height, err = jpegSize(decoded)
	}
	if err != nil {
		return err
	}
	if width < minImageSide || height < minImageSide || width > maxImageSide || height > maxImageSide {
		return errors.Errorf("%dx%d pixels: the API takes %d to %d pixels per side", width, height, minImageSide, maxImageSide)
	}
	return nil
}

// pngSize reads the dimensions from the IHDR chunk and rejects an animated
// PNG, which carries an acTL chunk before its image data.
func pngSize(data []byte) (width, height int, err error) {
	// Signature, then the IHDR chunk: length, type, width, height.
	if len(data) < 24 || string(data[12:16]) != "IHDR" {
		return 0, 0, errors.New("PNG without an IHDR chunk")
	}
	width = int(binary.BigEndian.Uint32(data[16:20]))
	height = int(binary.BigEndian.Uint32(data[20:24]))
	for offset := 8; offset+8 <= len(data); {
		length := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		chunkType := string(data[offset+4 : offset+8])
		switch chunkType {
		case "acTL":
			return 0, 0, errors.New("animated PNG: the API takes still images")
		case "IDAT", "IEND":
			return width, height, nil
		}
		// Chunk data and its CRC.
		if length < 0 || offset+12+length < offset {
			break
		}
		offset += 12 + length
	}
	return width, height, nil
}

// jpegSize reads the dimensions from the first frame header (SOF marker).
func jpegSize(data []byte) (width, height int, err error) {
	for offset := 2; offset+4 <= len(data); {
		if data[offset] != 0xff {
			return 0, 0, errors.New("JPEG without a frame header")
		}
		marker := data[offset+1]
		switch {
		case marker == 0xff:
			// Padding before a marker.
			offset++
			continue
		case marker == 0xd8 || marker == 0x01 || (marker >= 0xd0 && marker <= 0xd7):
			// Markers without a segment.
			offset += 2
			continue
		case marker == 0xda:
			return 0, 0, errors.New("JPEG without a frame header")
		}
		length := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		isSOF := marker >= 0xc0 && marker <= 0xcf && marker != 0xc4 && marker != 0xc8 && marker != 0xcc
		if isSOF {
			if offset+9 > len(data) {
				break
			}
			height = int(binary.BigEndian.Uint16(data[offset+5 : offset+7]))
			width = int(binary.BigEndian.Uint16(data[offset+7 : offset+9]))
			return width, height, nil
		}
		offset += 2 + length
	}
	return 0, 0, errors.New("JPEG without a frame header")
}

// isImageProperty says whether the property is a blob, whose content the
// module sends as an image. An unknown property is text.
func isImageProperty(cfg moduletools.ClassConfig, property string) bool {
	if cfg == nil || property == "" {
		return false
	}
	return cfg.PropertiesDataTypes()[property] == schema.DataTypeBlob
}

// checkImages fails on the first document at the given positions that is
// not an image the API takes, before any request is sent and billed.
func checkImages(documents []string, positions []int) error {
	for _, position := range positions {
		if err := checkImage(documents[position]); err != nil {
			return errors.Wrapf(err, "document %d: image", position)
		}
	}
	return nil
}

// imageBatches splits the positions of the images to judge into requests
// of at most MaxBatchImages images and MaxBatchImageChars of base64, in
// order. An image above that goes alone.
func imageBatches(indexes []int, documents []string) [][]int {
	var out [][]int
	var batch []int
	chars := 0
	for _, index := range indexes {
		length := len(documents[index])
		if len(batch) > 0 && (len(batch) == MaxBatchImages || chars+length > MaxBatchImageChars) {
			out = append(out, batch)
			batch, chars = nil, 0
		}
		batch = append(batch, index)
		chars += length
	}
	if len(batch) > 0 {
		out = append(out, batch)
	}
	return out
}

// assetsOf builds the assets of a request from image documents and returns
// the reference of each one, in order.
func assetsOf(documents []string) (map[string]imageAsset, []string, error) {
	assets := make(map[string]imageAsset, len(documents))
	references := make([]string, len(documents))
	for i, document := range documents {
		format, err := imageFormat(document)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "document %d: image", i)
		}
		reference := imageReference(i)
		assets[reference] = imageAsset{Type: assetTypeImage, DataBase64: document, Format: format}
		references[i] = reference
	}
	return assets, references, nil
}
