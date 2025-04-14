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

package clusterapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
)

type DbUsers struct {
	userManager *apikey.RemoteApiKey
	auth        auth
}

func NewDbUsers(manager *apikey.RemoteApiKey, auth auth) *DbUsers {
	return &DbUsers{userManager: manager, auth: auth}
}

func (d *DbUsers) Users() http.Handler {
	return d.auth.handleFunc(d.userHandler())
}

func (d *DbUsers) userHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch path {
		case "/cluster/users/db/lastUsedTime":
			if r.Method != http.MethodPost {
				msg := fmt.Sprintf("/user api path %q with method %v not found", path, r.Method)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			d.incomingUserStatus().ServeHTTP(w, r)
			return
		default:
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
	}
}

func (d *DbUsers) incomingUserStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var body apikey.UserStatusRequest
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			http.Error(w, "Error parsing JSON body", http.StatusBadRequest)
			return
		}
		userStatus, err := d.userManager.GetUserStatus(r.Context(), body)
		if err != nil {
			http.Error(w, "/user fulfill request: "+err.Error(), http.StatusBadRequest)
			return
		}

		if userStatus == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		userStatusBytes, err := json.Marshal(userStatus)
		if err != nil {
			http.Error(w, "/user marshal response: "+err.Error(),
				http.StatusInternalServerError)
		}

		w.Write(userStatusBytes)
	})
}
