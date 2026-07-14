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

// MaxUpfrontBodyAlloc caps how many bytes ReadBody allocates purely on the
// peer-supplied Content-Length claim. Bodies that declare more start from a
// buffer of this capacity and grow as data actually arrives, so a lying or
// buggy peer cannot force a large allocation without sending real bytes
// (io.ReadAll semantics). 64 MiB comfortably covers realistic internode
// payloads (a 100-object search response with 768-dim vectors is well under
// 1 MiB); larger legitimate bodies still work, they just pay the incremental
// growth above the cap.
const MaxUpfrontBodyAlloc = 64 << 20 // 64 MiB

// ReadBody reads an HTTP request or response body to EOF. When contentLength
// is positive (the Content-Length header was set) and within
// MaxUpfrontBodyAlloc, the result buffer is allocated at exactly that size
// upfront, avoiding the repeated grow-and-copy of io.ReadAll on large
// internode payloads. A non-positive contentLength (unknown length / chunked
// transfer encoding) falls back to io.ReadAll.
//
// net/http limits request and response body readers to the declared
// Content-Length, so a matching read is guaranteed to end at EOF. On the
// exact-size path, a body shorter than the declared length returns
// io.ErrUnexpectedEOF; above the cap, truncation surfaces in the payload
// decode instead (same as io.ReadAll).
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

	// The declared length exceeds what we are willing to allocate on the
	// peer's word alone: pre-size to the cap and grow only as data arrives.
	buf := bytes.NewBuffer(make([]byte, 0, MaxUpfrontBodyAlloc))
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
