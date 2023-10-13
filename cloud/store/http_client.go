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

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

var errLeaderChanged = errors.New("leader changed")

// TODO-RAFT
// check if we need to add  basic-auth

type HttpClient struct {
	client *http.Client
}

func NewHttpClient(httpClient *http.Client) {
}

type Joiner struct {
	ID    string `json:"id,omitempty"`
	Addr  string `json:"addr,omitempty"`
	Voter bool   `json:"voter"`
}

// Join makes a request to add joiner to the cluster
// In case the join was successful it returns the leader address. otherwise it returns an error
func (c *HttpClient) Join(ctx context.Context, leaderAddr string, joiner Joiner) (string, error) {
	joinerData, err := json.Marshal(joiner)
	if err != nil {
		return "", err
	}
	// follow the leader
	for {
		leaderAddr, err = c.join(ctx, leaderAddr, bytes.NewReader(joinerData))
		if !errors.Is(err, errLeaderChanged) {
			break
		}

	}
	return leaderAddr, err
}

// join attempts to join an existing cluster
func (c *HttpClient) join(ctx context.Context, leaderAddr string, body io.Reader) (string, error) {
	u := url.URL{Scheme: "http", Host: leaderAddr, Path: "/raft/join"}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return leaderAddr, nil
	case http.StatusMovedPermanently:
		leaderAddr = resp.Header.Get("location")
		if leaderAddr == "" {
			return "", fmt.Errorf("invalid redirect")
		}
		return leaderAddr, errLeaderChanged
	default:
		msg, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("status %s: %s", resp.Status, msg)
	}
}
