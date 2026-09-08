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

package replica

import (
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"

	"github.com/weaviate/weaviate/cluster/router/types"
)

// Digest payload encodings declared on the gRPC digest RPCs; zero keeps older
// peers on the repeated-proto path.
const (
	RepairDigestsEncodingProto  uint32 = 0
	RepairDigestsEncodingPacked uint32 = 1
)

// RepairDigestsToBinary encodes digests as fixed CompareDigestsRecordLength
// records — the shared wire format of the REST compareDigests endpoint and the
// gRPC packed encoding.
func RepairDigestsToBinary(digests []types.RepairDigest) []byte {
	out := make([]byte, 0, len(digests)*CompareDigestsRecordLength)
	var buf [CompareDigestsRecordLength]byte
	for _, d := range digests {
		copy(buf[:16], d.ID[:])
		binary.BigEndian.PutUint64(buf[16:24], uint64(d.UpdateTime))
		buf[24] = 0
		if d.Deleted {
			buf[24] = CompareDigestsFlagDeleted
		}
		out = append(out, buf[:]...)
	}
	return out
}

// RepairDigestsFromBinary decodes a RepairDigestsToBinary payload, rejecting
// any length that is not a whole number of records.
func RepairDigestsFromBinary(data []byte) ([]types.RepairDigest, error) {
	if len(data)%CompareDigestsRecordLength != 0 {
		return nil, fmt.Errorf("invalid packed digests length %d: not a multiple of %d",
			len(data), CompareDigestsRecordLength)
	}
	digests := make([]types.RepairDigest, len(data)/CompareDigestsRecordLength)
	for i := range digests {
		rec := data[i*CompareDigestsRecordLength : (i+1)*CompareDigestsRecordLength]
		id, err := uuid.FromBytes(rec[:16])
		if err != nil {
			return nil, fmt.Errorf("parse uuid from binary record: %w", err)
		}
		digests[i] = types.RepairDigest{
			ID:         id,
			UpdateTime: int64(binary.BigEndian.Uint64(rec[16:24])),
			Deleted:    rec[24]&CompareDigestsFlagDeleted != 0,
		}
	}
	return digests, nil
}
