//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package additional

import "github.com/go-openapi/strfmt"

type Classification struct {
	BasedOn          []string        `json:"basedOn"`
	ClassifiedFields []string        `json:"classifiedFields"`
	Completed        strfmt.DateTime `json:"completed,omitempty"`
	ID               strfmt.UUID     `json:"id,omitempty"`
	Scope            []string        `json:"scope"`
}

type Properties struct {
	Classification     bool                   `json:"classification"`
	RefMeta            bool                   `json:"refMeta"`
	Vector             bool                   `json:"vector"`
	Certainty          bool                   `json:"certainty"`
	ID                 bool                   `json:"id"`
	CreationTimeUnix   bool                   `json:"creationTimeUnix"`
	LastUpdateTimeUnix bool                   `json:"lastUpdateTimeUnix"`
	ModuleParams       map[string]interface{} `json:"moduleParams"`
	Distance           bool                   `json:"distance"`
	Score              bool                   `json:"score"`
}
