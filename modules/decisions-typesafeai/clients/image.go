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
)

// Image input is an experimental feature of the API: off by default, every
// request with an image switches it on with a header. The limits below are
// the API's, verified against it on 2026-10-15: the documentation says 256
// pixels per side, the API rejects 279.
const (
	dangerZoneImagesHeader = "TypeSafe-Danger-Zone-Images"
	assetTypeImage         = "image"
	formatPNG              = "png"
	formatJPEG             = "jpeg"

	maxImageBytes = 16 << 20
	maxImageChars = (maxImageBytes + 2) / 3 * 4
	minImageSide  = 280
	maxImageSide  = 1568

	// An image costs about one token per 375 pixels plus 270 (measured:
	// 280x280 is 476, 1568x1568 is 6,548) and the state of a batch counts
	// against the 32k tokens one question may see, so four of the largest
	// fit. The base64 of 24 MiB leaves room for the JSON in the API's 32
	// MiB body limit.
	MaxBatchImages     = 4
	MaxBatchImageChars = 24 << 20
)

type imageAsset struct {
	Type       string `json:"type"`
	DataBase64 string `json:"data_base64"`
	Format     string `json:"format"`
}

func imageReference(i int) string {
	return "image_" + strconv.Itoa(i)
}

var (
	pngMagic  = []byte("\x89PNG")
	jpegMagic = []byte("\xff\xd8\xff")
)

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

// checkImage fails where the API would reject the image, before anything
// is sent and billed.
func checkImage(encoded string) error {
	if len(encoded) > maxImageChars {
		return errors.Errorf("%d MB exceed the limit of %d MB per image", len(encoded)/4*3>>20, maxImageBytes>>20)
	}
	format, err := imageFormat(encoded)
	if err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return errors.New("not base64")
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

// pngSize reads the IHDR chunk. An animated PNG carries an acTL chunk
// before its image data; the API rejects it.
func pngSize(data []byte) (width, height int, err error) {
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
		if length < 0 || offset+12+length < offset {
			break
		}
		offset += 12 + length
	}
	return width, height, nil
}

// jpegSize reads the first frame header (SOF marker).
func jpegSize(data []byte) (width, height int, err error) {
	for offset := 2; offset+4 <= len(data); {
		if data[offset] != 0xff {
			return 0, 0, errors.New("JPEG without a frame header")
		}
		marker := data[offset+1]
		switch {
		case marker == 0xff:
			offset++
			continue
		case marker == 0xd8 || marker == 0x01 || (marker >= 0xd0 && marker <= 0xd7):
			// These markers have no segment.
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

func checkImages(documents []string, positions []int) error {
	for _, position := range positions {
		if err := checkImage(documents[position]); err != nil {
			return errors.Wrapf(err, "document %d: image", position)
		}
	}
	return nil
}

func assetsOf(documents []string) (map[string]imageAsset, []string, error) {
	assets := make(map[string]imageAsset, len(documents))
	references := make([]string, len(documents))
	for i, document := range documents {
		// checkImages ran on every document, so this cannot fail.
		format, err := imageFormat(document)
		if err != nil {
			return nil, nil, errors.Wrap(err, "image")
		}
		reference := imageReference(i)
		assets[reference] = imageAsset{Type: assetTypeImage, DataBase64: document, Format: format}
		references[i] = reference
	}
	return assets, references, nil
}
