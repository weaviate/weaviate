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

// DigestObjectsInRangeRecordLength is the byte size of one binary record on the
// digestsInRange endpoint: 16-byte UUID (RFC-4122 binary) + 8-byte UpdateTime
// (int64 big-endian). RepairResponse.Err/Deleted are not on the wire.
const DigestObjectsInRangeRecordLength = 24

// CompareDigestsRecordLength is the byte size of one binary record on the
// compareDigests endpoint: same as above plus a trailing flags byte (see
// CompareDigestsFlagDeleted).
const CompareDigestsRecordLength = 25

// CompareDigestsFlagDeleted is the flags-byte bit (byte 24) that mirrors
// RepairResponse.Deleted on the REST wire. Reserved for a future target-side
// deletion signal: the current target comparator never sets it (it reports
// tombstones as missing), so the bit is always zero today — but the REST
// encoder/decoder round-trips it so the protocol is ready.
const CompareDigestsFlagDeleted byte = 0x01

// CompareDigestsMaxBodyBytes caps the size of a CompareDigests REST request
// body. The cap leaves enough headroom for the largest realistic
// diffBatchSize × CompareDigestsRecordLength; tests guard the headroom.
const CompareDigestsMaxBodyBytes = 4 * 1024 * 1024
