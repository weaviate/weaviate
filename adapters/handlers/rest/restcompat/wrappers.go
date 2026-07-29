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

package restcompat

import "github.com/weaviate/weaviate/entities/models"

// Outer-field-wins JSON encoding: an outer field with the same json tag as a
// field on an embedded type takes precedence, so the embedded model is
// emitted verbatim plus the outer additions.
type replicationConfigOut struct {
	*models.ReplicationConfig
	AsyncEnabled bool `json:"asyncEnabled"`
}

type classOut struct {
	*models.Class
	ReplicationConfig *replicationConfigOut `json:"replicationConfig,omitempty"`
}

type schemaOut struct {
	*models.Schema
	Classes []*classOut `json:"classes"`
}

func wrapReplicationConfig(rc *models.ReplicationConfig) *replicationConfigOut {
	if rc == nil {
		return nil
	}
	return &replicationConfigOut{
		ReplicationConfig: rc,
		AsyncEnabled:      rc.Factor > 1 && !asyncReplicationGloballyDisabled.Load(),
	}
}

func wrapClass(c *models.Class) *classOut {
	if c == nil {
		return nil
	}
	return &classOut{
		Class:             c,
		ReplicationConfig: wrapReplicationConfig(c.ReplicationConfig),
	}
}

func wrapSchema(s *models.Schema) *schemaOut {
	if s == nil {
		return nil
	}
	// Preserve nilness — models.Schema.Classes has no omitempty, so a nil
	// slice must marshal as "classes":null, not "classes":[].
	var classes []*classOut
	if s.Classes != nil {
		classes = make([]*classOut, len(s.Classes))
		for i, c := range s.Classes {
			classes[i] = wrapClass(c)
		}
	}
	return &schemaOut{
		Schema:  s,
		Classes: classes,
	}
}
