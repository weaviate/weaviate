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

package httpv2

type modelMetadataResponse struct {
	Name     string       `json:"name"`
	Platform string       `json:"platform"`
	Backend  string       `json:"backend"`
	Versions []string     `json:"versions"`
	Inputs   []tensorSpec `json:"inputs"`
	Outputs  []tensorSpec `json:"outputs"`
}

type tensorSpec struct {
	Name     string   `json:"name"`
	Shape    []int32  `json:"shape"`
	Datatype DataType `json:"datatype"`
}

type modelMetadataError struct {
	Error string `json:"error"`
}
