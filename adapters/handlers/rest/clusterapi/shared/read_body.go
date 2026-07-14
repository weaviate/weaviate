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

package shared

import (
	"bytes"
	"io"
)

// MaxUpfrontBodyAlloc caps how many bytes ReadBody preallocates from a
// peer-supplied Content-Length; larger bodies grow incrementally instead
// (io.ReadAll semantics), so a lying or buggy peer can't force a large
// allocation without sending real bytes.
const MaxUpfrontBodyAlloc = 64 << 20 // 64 MiB

// ReadBody reads a body to EOF, preallocating an exact-size buffer when
// contentLength is positive and within MaxUpfrontBodyAlloc (net/http caps
// reads to the declared Content-Length, so short bodies surface as
// io.ErrUnexpectedEOF instead of hanging). Otherwise falls back to io.ReadAll.
func ReadBody(r io.Reader, contentLength int64) ([]byte, error) {
	if contentLength <= 0 {
		return io.ReadAll(r)
	}

	if contentLength <= MaxUpfrontBodyAlloc {
		buf := make([]byte, contentLength)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}

	// don't trust the declared length past the cap; grow incrementally instead
	buf := bytes.NewBuffer(make([]byte, 0, MaxUpfrontBodyAlloc))
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
