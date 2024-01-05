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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

type RemoteNode struct {
	client *http.Client
}

func NewRemoteNode(httpClient *http.Client) *RemoteNode {
	return &RemoteNode{client: httpClient}
}

func (c *RemoteNode) GetNodeStatus(ctx context.Context, hostName, className, output string) (*models.NodeStatus, error) {
	p := "/nodes/status"
	if className != "" {
		p = path.Join(p, className)
	}
	method := http.MethodGet
	params := url.Values{"output": []string{output}}
	url := url.URL{Scheme: "http", Host: hostName, Path: p, RawQuery: params.Encode()}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
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

	var nodeStatus models.NodeStatus
	err = json.Unmarshal(body, &nodeStatus)
	if err != nil {
		return nil, enterrors.NewErrUnmarshalBody(err)
	}

	return &nodeStatus, nil
}
