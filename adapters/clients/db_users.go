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

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
)

type RemoteUser struct {
	client       *http.Client
	nodeResolver nodeResolver
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

func NewRemoteUser(httpClient *http.Client, nodeResolver nodeResolver) *RemoteUser {
	return &RemoteUser{client: httpClient, nodeResolver: nodeResolver}
}

func (c *RemoteUser) GetAndUpdateLastUsedTime(ctx context.Context, nodeName string, users map[string]time.Time, returnStatus bool) (*apikey.UserStatusResponse, error) {
	p := "/cluster/users/db/lastUsedTime"
	method := http.MethodPost
	hostName, found := c.nodeResolver.NodeHostname(nodeName)
	if !found {
		return nil, fmt.Errorf("unable to resolve hostname for %s", nodeName)
	}
	url := url.URL{Scheme: "http", Host: hostName, Path: p}

	jsonBody, err := json.Marshal(apikey.UserStatusRequest{Users: users, ReturnStatus: returnStatus})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, enterrors.NewErrOpenHttpRequest(err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, enterrors.NewErrSendHttpRequest(err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return nil, enterrors.NewErrUnexpectedStatusCode(res.StatusCode, body)
	}

	var userStatus apikey.UserStatusResponse
	err = json.Unmarshal(body, &userStatus)
	if err != nil {
		return nil, enterrors.NewErrUnmarshalBody(err)
	}

	return &userStatus, nil
}
