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

package clusterapi

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"

	"github.com/weaviate/weaviate/entities/userindex"
)

const (
	urlPatternIndexes = `\/userindexes\/([A-Za-z0-9_+-]+)`
)

type UserIndexes struct {
	uis               userIndexStatuser
	regexpUserIndexes *regexp.Regexp
}

func NewUserIndexes(uis userIndexStatuser) *UserIndexes {
	return &UserIndexes{
		uis:               uis,
		regexpUserIndexes: regexp.MustCompile(urlPatternIndexes),
	}
}

func (ui *UserIndexes) RootHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case ui.regexpUserIndexes.MatchString(path):
			if r.Method != http.MethodGet {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			ui.getAll().ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
			return
		}
	})
}

func (ui *UserIndexes) getAll() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := ui.regexpUserIndexes.FindStringSubmatch(r.URL.Path)
		if len(args) != 2 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index := args[1]

		list, err := ui.uis.UserIndexStatus(r.Context(), index)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		marshalled, err := UserIndexPayloads.List.Marshal(list)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		UserIndexPayloads.List.SetContentTypeHeader(w)
		w.Write(marshalled)
	})
}

var UserIndexPayloads = userIndexPayloads{}

type userIndexPayloads struct {
	List userIndexListPayload
}

type userIndexStatuser interface {
	UserIndexStatus(ctx context.Context, class string) ([]userindex.Index, error)
}

type userIndexListPayload struct{}

func (p userIndexListPayload) MIME() string {
	return "application/vnd.weaviate.userindex+json"
}

func (p userIndexListPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p userIndexListPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

func (p userIndexListPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p userIndexListPayload) Marshal(list []userindex.Index) ([]byte, error) {
	return json.Marshal(list)
}
