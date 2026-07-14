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
	"errors"
	"io"
)

// UnverifiedBodyAlloc is the only allocation ReadBody makes on an
// unverified Content-Length claim. Keeping it small bounds the memory a
// peer can pin with stalled connections (clusterapi auth can be a noop,
// so claims alone must stay cheap) while still giving the common small
// internode body a single exact-size allocation.
const UnverifiedBodyAlloc = 1 << 20 // 1 MiB

// ReadBody reads a body to EOF with allocations proportional to the bytes
// actually delivered, not to the peer-supplied Content-Length. Bodies within
// UnverifiedBodyAlloc get a single exact-size allocation. Larger claims buy
// only an UnverifiedBodyAlloc head; from there the buffer doubles (capped at
// the claim) as bytes arrive, so live memory never exceeds 2x what the peer
// has actually sent. Honest large bodies pay O(log n) grow-and-copy steps
// and still end in an exact-size buffer. A non-positive contentLength falls
// back to io.ReadAll. net/http caps reads at the declared Content-Length, so
// bodies shorter than declared surface as io.ErrUnexpectedEOF instead of
// hanging.
func ReadBody(r io.Reader, contentLength int64) ([]byte, error) {
	if contentLength <= 0 {
		return io.ReadAll(r)
	}

	if contentLength <= UnverifiedBodyAlloc {
		buf := make([]byte, contentLength)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}

	// the claim exceeds what we allocate on trust: start with an
	// UnverifiedBodyAlloc head and grow only as bytes actually arrive
	buf := make([]byte, UnverifiedBodyAlloc)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	for int64(len(buf)) < contentLength {
		grown := make([]byte, min(2*int64(len(buf)), contentLength))
		filled := copy(grown, buf)
		buf = grown // drop the old buffer before blocking on the next read
		if _, err := io.ReadFull(r, buf[filled:]); err != nil {
			if errors.Is(err, io.EOF) {
				// ReadFull reports a bare EOF when zero bytes arrive, but
				// mid-body (the head already arrived) that is a truncation
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return buf, nil
}
