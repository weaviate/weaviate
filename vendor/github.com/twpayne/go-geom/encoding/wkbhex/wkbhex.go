// Package wkbhex implements Well Known Binary encoding and decoding of
// strings.
package wkbhex

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
)

// Encode encodes an arbitrary geometry to a string.
func Encode(g geom.T, byteOrder binary.ByteOrder) (string, error) {
	wkb, err := wkb.Marshal(g, byteOrder)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(wkb), nil
}

// Decode decodes an arbitrary geometry from a string.
func Decode(s string) (geom.T, error) {
	data, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return wkb.Unmarshal(data)
}
