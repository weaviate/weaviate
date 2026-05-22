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

package changelog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	flagDelete uint8 = 1 << 0

	// On-disk frame:
	//	 [1B  Version		   currently 0]
	//   [8B  LSN              big-endian]
	//   [1B  flags            bit 0 = isDelete]
	//   [8B  updateTimeMillis little-endian]
	//   [16B uuid 			   UUID]
	//   [4B  payloadLen       little-endian]
	//   [N   payload          bytes]
	//   [4B  CRC32(IEEE) little-endian, over all preceding bytes]
	headerSize           = 1 + 8 + 1 + 8 + 16 + 4
	trailerSize          = 4
	EncodeVersion0 uint8 = 0
)

// Entry is a single change-capture record describing a PUT or DELETE applied
// to the source shard. Payload is opaque to this package; Phase 2 wires
// VObject V2 bytes into it.
type Entry struct {
	Version          uint8
	LSN              uint64
	IsDelete         bool
	UpdateTimeMillis int64
	UUID             [16]byte
	Payload          []byte
}

// Encode serialises e into dst, growing dst if it lacks capacity, and returns
// the used slice. dst may be nil. Call sites that hold a reusable scratch
// buffer should pass it in as dst[:0] to avoid an allocation when the buffer
// is large enough.
func Encode(dst []byte, e *Entry) ([]byte, error) {
	total := headerSize + len(e.Payload) + trailerSize
	if cap(dst) < total {
		dst = make([]byte, total)
	} else {
		dst = dst[:total]
	}

	// Version is currently unused but reserved for future schema changes
	dst[0] = EncodeVersion0

	// LSN is big-endian per the spec; byteops is little-endian only.
	binary.BigEndian.PutUint64(dst[1:9], e.LSN)

	rw := byteops.NewReadWriterWithOps(dst, byteops.WithPosition(9))
	var flags uint8
	if e.IsDelete {
		flags |= flagDelete
	}
	rw.WriteByte(flags)
	rw.WriteUint64(uint64(e.UpdateTimeMillis))
	if err := rw.CopyBytesToBuffer(e.UUID[:]); err != nil {
		return nil, fmt.Errorf("copy UUID to buffer: %w", err)
	}
	rw.WriteUint32(uint32(len(e.Payload)))
	if len(e.Payload) > 0 {
		if err := rw.CopyBytesToBuffer(e.Payload); err != nil {
			return nil, fmt.Errorf("copy payload to buffer: %w", err)
		}
	}

	crc := crc32.ChecksumIEEE(dst[:headerSize+len(e.Payload)])
	binary.LittleEndian.PutUint32(dst[headerSize+len(e.Payload):], crc)
	return dst, nil
}

// DecodeFrame reads one frame from r.
//
// Returns io.EOF at a clean frame boundary (no more frames), and also when r
// returns a partial frame ("torn tail") — callers iterating an append-only
// log can treat both cases as "stop reading, everything prior is valid".
// Returns ErrCRCMismatch when the frame's CRC32 does not match its contents.
func DecodeFrame(r io.Reader) (*Entry, error) {
	header := make([]byte, headerSize)
	n, err := io.ReadFull(r, header)
	switch {
	case err == io.EOF && n == 0:
		return nil, io.EOF
	case errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF):
		return nil, io.EOF
	case err != nil:
		return nil, err
	}

	version := uint8(header[0])
	lsn := binary.BigEndian.Uint64(header[1:9])
	flags := header[9]
	ts := int64(binary.LittleEndian.Uint64(header[10:18]))
	var id [16]byte
	copy(id[:], header[18:34])
	payloadLen := binary.LittleEndian.Uint32(header[34:38])

	body := make([]byte, int(payloadLen)+trailerSize)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}

	gotCRC := binary.LittleEndian.Uint32(body[payloadLen:])

	h := crc32.NewIEEE()
	_, _ = h.Write(header)
	_, _ = h.Write(body[:payloadLen])
	if h.Sum32() != gotCRC {
		return nil, ErrCRCMismatch
	}

	// Return nil (not an empty slice) for zero-length payloads so callers can
	// unambiguously distinguish "no payload" (deletes) from "present but
	// empty". IsDelete is the intended discriminator, but this removes the
	// nil-vs-empty-slice foot-gun for anyone who reaches for Payload first.
	var payload []byte
	if payloadLen > 0 {
		payload = body[:payloadLen]
	}

	return &Entry{
		Version:          version,
		LSN:              lsn,
		IsDelete:         flags&flagDelete != 0,
		UpdateTimeMillis: ts,
		UUID:             id,
		Payload:          payload,
	}, nil
}
