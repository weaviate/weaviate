//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
)

// VObject is a versioned object for detecting replication inconsistencies
type VObject struct {
	// LatestObject is to most up-to-date version of an object
	LatestObject *models.Object `json:"object,omitempty"`

	// StaleUpdateTime is the LastUpdateTimeUnix of the stale object sent to the coordinator
	StaleUpdateTime int64 `json:"updateTime,omitempty"`

	// Version is the most recent incremental version number of the object
	Version uint64 `json:"version"`
}

// vobjectMarshaler is a helper for the functions implementing encoding.BinaryMarshaler
//
// Because models.Object has an optimized custom MarshalBinary function, that is what
// we want to use when serializing, rather than json.Marshal. This is just a thin
// wrapper around the storobj bytes resulting from the underlying call to MarshalBinary
type vobjectMarshaler struct {
	StaleUpdateTime int64
	Version         uint64
	LatestObject    []byte
}

func (vo *VObject) MarshalBinary() ([]byte, error) {
	obj, err := vo.LatestObject.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal object: %w", err)
	}

	b := vobjectMarshaler{vo.StaleUpdateTime, vo.Version, obj}
	return json.Marshal(b)
}

func (vo *VObject) UnmarshalBinary(data []byte) error {
	var b vobjectMarshaler

	err := json.Unmarshal(data, &b)
	if err != nil {
		return err
	}
	vo.StaleUpdateTime = b.StaleUpdateTime
	vo.Version = b.Version

	var obj models.Object
	err = obj.UnmarshalBinary(b.LatestObject)
	if err != nil {
		return fmt.Errorf("unmarshal object: %w", err)
	}
	vo.LatestObject = &obj

	return nil
}
