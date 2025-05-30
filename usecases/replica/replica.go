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

package replica

import (
	"encoding/json"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/storobj"
)

// Replica represents a replicated data item
type Replica struct {
	ID                      strfmt.UUID     `json:"id,omitempty"`
	Deleted                 bool            `json:"deleted"`
	Object                  *storobj.Object `json:"object,omitempty"`
	LastUpdateTimeUnixMilli int64           `json:"lastUpdateTimeUnixMilli"`
}

// robjectMarshaler is a helper for the methods implementing encoding.BinaryMarshaler
//
// Because *storobj.Object has an optimized custom MarshalBinary method, that is what
// we want to use when serializing, rather than json.Marshal. This is just a thin
// wrapper around the storobj bytes resulting from the underlying call to MarshalBinary
type robjectMarshaler struct {
	ID                      strfmt.UUID
	Deleted                 bool
	LastUpdateTimeUnixMilli int64
	Object                  []byte
}

func (r *Replica) MarshalBinary() ([]byte, error) {
	b := robjectMarshaler{
		ID:                      r.ID,
		Deleted:                 r.Deleted,
		LastUpdateTimeUnixMilli: r.LastUpdateTimeUnixMilli,
	}
	if r.Object != nil {
		obj, err := r.Object.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal object: %w", err)
		}
		b.Object = obj
	}

	return json.Marshal(b)
}

func (r *Replica) UnmarshalBinary(data []byte) error {
	var b robjectMarshaler

	err := json.Unmarshal(data, &b)
	if err != nil {
		return err
	}
	r.ID = b.ID
	r.Deleted = b.Deleted
	r.LastUpdateTimeUnixMilli = b.LastUpdateTimeUnixMilli

	if b.Object != nil {
		var obj storobj.Object
		err = obj.UnmarshalBinary(b.Object)
		if err != nil {
			return fmt.Errorf("unmarshal object: %w", err)
		}
		r.Object = &obj
	}

	return nil
}

type Replicas []Replica

func (ro Replicas) MarshalBinary() ([]byte, error) {
	ms := make([]robjectMarshaler, len(ro))

	for i, obj := range ro {
		m := robjectMarshaler{
			ID:                      obj.ID,
			Deleted:                 obj.Deleted,
			LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnixMilli,
		}
		if obj.Object != nil {
			b, err := obj.Object.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("marshal object %q: %w", obj.ID, err)
			}
			m.Object = b
		}
		ms[i] = m
	}

	return json.Marshal(ms)
}

func (ro *Replicas) UnmarshalBinary(data []byte) error {
	var ms []robjectMarshaler

	err := json.Unmarshal(data, &ms)
	if err != nil {
		return err
	}

	reps := make(Replicas, len(ms))
	for i, m := range ms {
		rep := Replica{
			ID:                      m.ID,
			Deleted:                 m.Deleted,
			LastUpdateTimeUnixMilli: m.LastUpdateTimeUnixMilli,
		}
		if m.Object != nil {
			var obj storobj.Object
			err = obj.UnmarshalBinary(m.Object)
			if err != nil {
				return fmt.Errorf("unmarshal object %q: %w", m.ID, err)
			}
			rep.Object = &obj
		}
		reps[i] = rep
	}

	*ro = reps
	return nil
}

// UpdateTime return update time if it exists and 0 otherwise
func (r Replica) UpdateTime() int64 {
	if r.Object != nil {
		return r.Object.LastUpdateTimeUnix()
	}
	return r.LastUpdateTimeUnixMilli
}
