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

package generate

type Params struct {
	Prompt     *string
	Task       *string
	Properties []string
}

func (n Params) GetPrompt() string {
	return *n.Prompt
}

func (n Params) GetTask() string {
	return *n.Task
}

func (n Params) GetProperties() []string {
	return n.Properties
}
