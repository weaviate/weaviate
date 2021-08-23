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

package tokens

type Params struct {
	Limit      *int     // optional parameter
	Certainty  *float64 // optional parameter
	Properties []string
}

func (n Params) GetCertainty() float64 {
	return *n.Certainty
}

func (n Params) GetLimit() int {
	return *n.Limit
}

func (n Params) GetProperties() []string {
	return n.Properties
}
