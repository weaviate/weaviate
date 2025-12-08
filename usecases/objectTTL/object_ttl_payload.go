//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objectTTL

import (
	"encoding/json"
	"net/http"
	"time"
)

type ObjectsExpiredStatusResponsePayload struct {
	DeletionOngoing bool `json:"deletion_ongoing"`
}

func (p ObjectsExpiredStatusResponsePayload) MIME() string {
	return "application/vnd.weaviate.objectsexpired+json"
}

func (p ObjectsExpiredStatusResponsePayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p ObjectsExpiredStatusResponsePayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

func (p ObjectsExpiredStatusResponsePayload) Marshal(deletionOngoing bool) ([]byte, error) {
	p.DeletionOngoing = deletionOngoing

	return json.Marshal(p)
}

func (p ObjectsExpiredStatusResponsePayload) Unmarshal(in []byte) (bool, error) {
	err := json.Unmarshal(in, &p)
	return p.DeletionOngoing, err
}

func (p ObjectsExpiredStatusResponsePayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

type ObjectsExpiredPayload struct {
	Class    string `json:"class"`
	Prop     string `json:"prop"`
	TtlMilli int64  `json:"ttlMilli"`
	DelMilli int64  `json:"delMilli"`
}

func (p ObjectsExpiredPayload) MIME() string {
	return "application/vnd.weaviate.objectsexpired+json"
}

// func (p objectsExpiredPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
// 	ct := r.Header.Get("content-type")
// 	return ct, ct == p.MIME()
// }

func (p ObjectsExpiredPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

// func (p objectsExpiredPayload) SetContentTypeHeader(w http.ResponseWriter) {
// 	w.Header().Set("content-type", p.MIME())
// }

func (p ObjectsExpiredPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

func (p ObjectsExpiredPayload) Marshal(propName string, ttlThreshold, deletionTime time.Time) ([]byte, error) {
	p.Prop = propName
	p.TtlMilli = ttlThreshold.UnixMilli()
	p.DelMilli = deletionTime.UnixMilli()

	return json.Marshal(p)
}

func (p ObjectsExpiredPayload) Unmarshal(in []byte) (propName string, ttlThreshold, deletionTime time.Time, err error) {
	err = json.Unmarshal(in, &p)
	return p.Prop, time.UnixMilli(p.TtlMilli), time.UnixMilli(p.DelMilli), err
}
