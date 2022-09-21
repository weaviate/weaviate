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

package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/semi-technologies/weaviate/entities/models"
)

type RemoteNode struct {
	client *http.Client
}

func NewRemoteNode(httpClient *http.Client) *RemoteNode {
	return &RemoteNode{client: httpClient}
}

func (c *RemoteNode) GetNodeStatus(ctx context.Context, hostName string,
) (*models.NodeStatus, error) {
	path := "/nodes/status"
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("open http request: %w", err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send http request: %w", err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	var nodeStatus models.NodeStatus
	err = json.Unmarshal(body, &nodeStatus)
	if err != nil {
		return nil, fmt.Errorf("unmarshal body: %w", err)
	}

	return &nodeStatus, nil
}
