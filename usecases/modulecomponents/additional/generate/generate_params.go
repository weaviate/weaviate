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

package generate

type Params struct {
	Prompt              *string
	Task                *string
	Properties          []string
	PropertiesToExtract []string
	Debug               bool
	Options             map[string]interface{}
}

func (n Params) GetPropertiesToExtract() []string {
	return n.PropertiesToExtract
}
