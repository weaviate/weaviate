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

// DigestObjectsInRangeRecordLength is the size in bytes of a single binary
// digest record used by the digestsInRange endpoint.
// Each record encodes:
//
//	bytes  0–15  UUID (RFC-4122 binary form, big-endian)
//	bytes 16–23  UpdateTime (int64 big-endian, Unix milliseconds)
//
// The Err and Deleted fields of RepairResponse are not part of the wire
// format and are therefore omitted — ObjectDigestsInRange never populates them.
const DigestObjectsInRangeRecordLength = 24

