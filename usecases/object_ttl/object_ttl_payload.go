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

package objectttl

import (
	"net/http"
)

type ObjectsExpiredStatusResponse struct {
	DeletionOngoing bool `json:"deletion_ongoing"`
}

func (p ObjectsExpiredStatusResponse) MIME() string {
	return "application/vnd.weaviate.objectsexpired+json"
}

func (p ObjectsExpiredStatusResponse) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p ObjectsExpiredStatusResponse) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

// ----------------------------------------------------------------------------

type ObjectsExpiredPayload struct {
	Class        string `json:"class"`
	ClassVersion uint64 `json:"class_version"`
	Prop         string `json:"prop"`
	TtlMilli     int64  `json:"ttlMilli"`
	DelMilli     int64  `json:"delMilli"`
}

func (p ObjectsExpiredPayload) MIME() string {
	return "application/vnd.weaviate.objectsexpired+json"
}

func (p ObjectsExpiredPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p ObjectsExpiredPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

// ----------------------------------------------------------------------------

type ObjectsExpiredAbortResponse struct {
	Aborted bool `json:"deletion_ongoing"`
}

func (p ObjectsExpiredAbortResponse) MIME() string {
	return "application/vnd.weaviate.objectsexpired+json"
}

func (p ObjectsExpiredAbortResponse) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p ObjectsExpiredAbortResponse) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}
