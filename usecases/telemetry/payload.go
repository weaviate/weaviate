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

package telemetry

import (
	"github.com/go-openapi/strfmt"
)

// PayloadType is the discrete set of statuses which indicate the type of payload sent
var PayloadType = struct {
	Init      string
	Update    string
	Terminate string
}{
	Init:      "INIT",
	Update:    "UPDATE",
	Terminate: "TERMINATE",
}

// Payload is the object transmitted for telemetry purposes
type Payload struct {
	MachineID        strfmt.UUID `json:"machineId"`
	Type             string      `json:"type"`
	Version          string      `json:"version"`
	ObjectsCount     int64       `json:"objs"`
	OS               string      `json:"os"`
	Arch             string      `json:"arch"`
	UsedModules      []string    `json:"usedModules,omitempty"`
	CollectionsCount int         `json:"collectionsCount"`
}
