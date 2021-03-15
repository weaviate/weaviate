//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package additional

type Interpretation struct {
	Source []*InterpretationSource `json:"source"`
}

type InterpretationSource struct {
	Concept    string  `json:"concept,omitempty"`
	Occurrence uint64  `json:"occurrence,omitempty"`
	Weight     float64 `json:"weight,omitempty"`
}
