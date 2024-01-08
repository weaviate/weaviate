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

package sharding

import "encoding/json"

func (s *State) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func StateFromJSON(in []byte, nodes nodes) (*State, error) {
	s := State{}

	if err := json.Unmarshal(in, &s); err != nil {
		return nil, err
	}

	s.localNodeName = nodes.LocalName()

	return &s, nil
}
