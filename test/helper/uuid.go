//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helper

import (
	"encoding/binary"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
)

// InToUUID takes an unsigned int64 and places it in BigEndian fashion into the
// upper 8 bytes of a 16 byte UUID. This makes it easy to produce easy-to-read
// UUIDs in test scenarios. For example:
//
//	IntToUUID(1)
//	// returns "00000000-0000-0000-0000-000000000001"
func IntToUUID(in uint64) strfmt.UUID {
	id := [16]byte{}
	binary.BigEndian.PutUint64(id[8:16], in)
	return strfmt.UUID(uuid.UUID(id).String())
}
