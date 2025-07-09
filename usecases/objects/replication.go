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

package objects

import (
	"encoding/json"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
)

// VObject is a versioned object for detecting replication inconsistencies
type VObject struct {
	// ID of the Object.
	// Format: uuid
	ID strfmt.UUID `json:"id,omitempty"`

	Deleted bool `json:"deleted"`

	// Timestamp of the last Object update in milliseconds since epoch UTC.
	LastUpdateTimeUnixMilli int64 `json:"lastUpdateTimeUnixMilli,omitempty"`

	// LatestObject is to most up-to-date version of an object
	LatestObject *models.Object `json:"object,omitempty"`

	Vector       []float32              `json:"vector"`
	Vectors      map[string][]float32   `json:"vectors"`
	MultiVectors map[string][][]float32 `json:"multiVectors"`

	// StaleUpdateTime is the LastUpdateTimeUnix of the stale object sent to the coordinator
	StaleUpdateTime int64 `json:"updateTime,omitempty"`

	// Version is the most recent incremental version number of the object
	Version uint64 `json:"version"`
}

// vobjectMarshaler is a helper for the methods implementing encoding.BinaryMarshaler
//
// Because models.Object has an optimized custom MarshalBinary method, that is what
// we want to use when serializing, rather than json.Marshal. This is just a thin
// wrapper around the model bytes resulting from the underlying call to MarshalBinary
type vobjectMarshaler struct {
	ID                      strfmt.UUID
	Deleted                 bool
	LastUpdateTimeUnixMilli int64
	StaleUpdateTime         int64
	Version                 uint64
	Vector                  []float32
	Vectors                 map[string][]float32
	MultiVectors            map[string][][]float32
	LatestObject            []byte
}

func (vo *VObject) MarshalBinary() ([]byte, error) {
	b := vobjectMarshaler{
		ID:                      vo.ID,
		Deleted:                 vo.Deleted,
		LastUpdateTimeUnixMilli: vo.LastUpdateTimeUnixMilli,
		StaleUpdateTime:         vo.StaleUpdateTime,
		Vector:                  vo.Vector,
		Vectors:                 vo.Vectors,
		MultiVectors:            vo.MultiVectors,
		Version:                 vo.Version,
	}
	if vo.LatestObject != nil {
		obj, err := vo.LatestObject.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal object: %w", err)
		}
		b.LatestObject = obj
	}

	return json.Marshal(b)
}

func (vo *VObject) UnmarshalBinary(data []byte) error {
	var b vobjectMarshaler

	err := json.Unmarshal(data, &b)
	if err != nil {
		return err
	}

	vo.ID = b.ID
	vo.Deleted = b.Deleted
	vo.LastUpdateTimeUnixMilli = b.LastUpdateTimeUnixMilli
	vo.StaleUpdateTime = b.StaleUpdateTime
	vo.Vector = b.Vector
	vo.Vectors = b.Vectors
	vo.MultiVectors = b.MultiVectors
	vo.Version = b.Version

	if b.LatestObject != nil {
		var obj models.Object
		err = obj.UnmarshalBinary(b.LatestObject)
		if err != nil {
			return fmt.Errorf("unmarshal object: %w", err)
		}
		vo.LatestObject = &obj
	}

	return nil
}
