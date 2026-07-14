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
	"fmt"
	"io"
	"math"
)

// ReadBody reads an HTTP request or response body to EOF. When contentLength
// is positive (the Content-Length header was set), the result buffer is
// allocated at exactly that size upfront, avoiding the repeated grow-and-copy
// of io.ReadAll on large internode payloads. A non-positive contentLength
// (unknown length / chunked transfer encoding) falls back to io.ReadAll.
//
// net/http limits request and response body readers to the declared
// Content-Length, so a matching read is guaranteed to end at EOF. A body
// shorter than the declared length returns io.ErrUnexpectedEOF.
func ReadBody(r io.Reader, contentLength int64) ([]byte, error) {
	if contentLength <= 0 {
		return io.ReadAll(r)
	}
	if contentLength > int64(math.MaxInt) {
		return nil, fmt.Errorf("content length %d overflows int", contentLength)
	}

	buf := make([]byte, contentLength)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
