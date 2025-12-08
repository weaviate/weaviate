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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type RemoteObjectTTL struct {
	client       *http.Client
	nodeResolver nodeResolver
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

func NewRemoteObjectTTL(httpClient *http.Client, nodeResolver nodeResolver) *RemoteObjectTTL {
	return &RemoteObjectTTL{client: httpClient, nodeResolver: nodeResolver}
}

func (c *RemoteObjectTTL) CheckIfStillRunning(ctx context.Context, nodeName string) (bool, error) {
	p := "/cluster/objectTTL/status"
	method := http.MethodGet
	hostName, found := c.nodeResolver.NodeHostname(nodeName)
	if !found {
		return false, fmt.Errorf("unable to resolve hostname for %s", nodeName)
	}
	url := url.URL{Scheme: "http", Host: hostName, Path: p}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return false, enterrors.NewErrOpenHttpRequest(err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return false, enterrors.NewErrSendHttpRequest(err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return false, enterrors.NewErrUnexpectedStatusCode(res.StatusCode, body)
	}

	var stilRunning ObjectsExpiredStatusResponsePayload
	err = json.Unmarshal(body, &stilRunning)
	if err != nil {
		return false, enterrors.NewErrUnmarshalBody(err)
	}

	return stilRunning.DeletionOngoing, nil
}

func (c *RemoteObjectTTL) StartRemoteDelete(ctx context.Context, nodeName string, classes []ObjectsExpiredPayload) error {
	p := "/cluster/objectTTL/deleteExpired"
	method := http.MethodPost
	hostName, found := c.nodeResolver.NodeHostname(nodeName)
	if !found {
		return fmt.Errorf("unable to resolve hostname for %s", nodeName)
	}
	url := url.URL{Scheme: "http", Host: hostName, Path: p}

	jsonBody, err := json.Marshal(classes)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), bytes.NewBuffer(jsonBody))
	if err != nil {
		return enterrors.NewErrOpenHttpRequest(err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return enterrors.NewErrSendHttpRequest(err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return enterrors.NewErrUnexpectedStatusCode(res.StatusCode, body)
	}

	return nil
}
