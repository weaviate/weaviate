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

const (
	// MaxOnArrivalBodyAlloc caps how many bytes ReadBody allocates based on
	// the peer-supplied Content-Length once the peer has proven itself by
	// delivering UnverifiedBodyAlloc real bytes; claims beyond the cap grow
	// incrementally (io.ReadAll semantics) as data arrives.
	MaxOnArrivalBodyAlloc = 64 << 20 // 64 MiB

	// UnverifiedBodyAlloc is the only allocation ReadBody makes on an
	// unverified Content-Length claim. Keeping it small bounds the memory a
	// peer can pin with stalled connections (clusterapi auth can be a noop,
	// so claims alone must stay cheap) while still giving the common small
	// internode body a single exact-size allocation.
	UnverifiedBodyAlloc = 1 << 20 // 1 MiB
)

// ReadBody reads a body to EOF with allocations proportional to trust: at
// most UnverifiedBodyAlloc is allocated on the Content-Length claim alone;
// only after those bytes actually arrive does the claim buy an exact-size
// buffer (up to MaxOnArrivalBodyAlloc), so forcing a multi-MiB allocation
// costs the peer real bandwidth. Bodies within UnverifiedBodyAlloc get a
// single exact-size allocation. A non-positive contentLength falls back to
// io.ReadAll. net/http caps reads at the declared Content-Length, so bodies
// shorter than declared surface as io.ErrUnexpectedEOF instead of hanging.
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

	// the claim exceeds what we allocate on trust: require the first
	// UnverifiedBodyAlloc bytes to arrive before allocating any further
	head := make([]byte, UnverifiedBodyAlloc)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, err
	}

	if contentLength <= MaxOnArrivalBodyAlloc {
		buf := make([]byte, contentLength)
		copy(buf, head)
		if _, err := io.ReadFull(r, buf[len(head):]); err != nil {
			return nil, err
		}
		return buf, nil
	}

	// don't trust the declared length past the cap; grow incrementally instead
	buf := bytes.NewBuffer(make([]byte, 0, MaxOnArrivalBodyAlloc))
	buf.Write(head)
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
