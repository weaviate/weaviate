//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go generate; DO NOT EDIT.
// This file was generated by go generate ./deprecations at 2020-09-15
package deprecations

import (
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

func timeMust(t time.Time, err error) strfmt.DateTime {
	if err != nil {
		panic(err)
	}

	return strfmt.DateTime(t)
}

func timeMustPtr(t time.Time, err error) *strfmt.DateTime {
	if err != nil {
		panic(err)
	}

	parsed := strfmt.DateTime(t)
	return &parsed
}

func ptString(in string) *string {
	return &in
}

var ByID = map[string]models.Deprecation{
	"rest-meta-prop": models.Deprecation{
		ID:           "rest-meta-prop",
		Status:       "deprecated",
		APIType:      "REST",
		Mitigation:   "Use ?include=<propName>, e.g. ?include=_classification for classification meta or ?include=_vector to show the vector position or ?include=_classification,_vector for both. When consuming the response use the underscore fields such as _vector, as the meta object in the reponse, such as meta.vector will be removed.",
		Msg:          "use of deprecated property ?meta=true/false",
		SinceVersion: "0.22.8",
		SinceTime:    timeMust(time.Parse(time.RFC3339, "2020-06-15T16:18:06.000Z")),
	},
	"config-files": models.Deprecation{
		ID:           "config-files",
		Status:       "deprecated",
		APIType:      "Configuration",
		Mitigation:   "Configure Weaviate using environment variables.",
		Msg:          "use of deprecated command line argument --config-file",
		SinceVersion: "0.22.16",
		SinceTime:    timeMust(time.Parse(time.RFC3339, "2020-09-08T09:46:00.000Z")),
	},
}
