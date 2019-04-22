/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package http_client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/prometheus/common/log"
	"golang.org/x/net/context/ctxhttp"
)

type gremlin_http_query struct {
	Gremlin string `json:"gremlin"`
}

type gremlinResponseStatus struct {
	Message string `json:"string"`
	Code    int    `json:"code"`
}

type gremlinResponseResult struct {
	Data []interface{} `json:"data"`
	Meta interface{}   `json:"meta"`
}

type gremlinResponse struct {
	Status gremlinResponseStatus `json:"status"`
	Result gremlinResponseResult `json:"result"`
}

func (c *Client) Execute(ctx context.Context, query gremlin.Gremlin) (*gremlin.Response, error) {
	queryString := query.String()

	q := gremlin_http_query{
		Gremlin: queryString,
	}

	json_bytes, err := json.Marshal(&q)
	if err != nil {
		log.Errorf("Could not create query, because %v", err)
		return nil, fmt.Errorf("Could not create query because %v", err)
	}

	bytesReader := bytes.NewReader(json_bytes)
	req, err := http.NewRequest("POST", c.endpoint, bytesReader)
	if err != nil {
		log.Errorf("Could not create HTTP request, because %v", err)
		return nil, fmt.Errorf("Could not create HTTP request to resolve a Gremlin query; %v", err)
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	res, err := ctxhttp.Do(ctx, &c.client, req)
	if err != nil {
		return nil, fmt.Errorf("Could not peform HTTP request to JanusGraph, because %v", err)
	}

	defer res.Body.Close()
	buf, err := ioutil.ReadAll(res.Body)
	var resData gremlinResponse
	json.Unmarshal(buf, &resData)

	switch res.StatusCode {
	case 200:
		data := make([]gremlin.Datum, 0)

		for _, d := range resData.Result.Data {
			data = append(data, gremlin.Datum{Datum: d})
		}

		clientRes := gremlin.Response{Data: data}
		return &clientRes, nil
	case 500:
		return nil, fmt.Errorf("server error: %s", string(buf))
	default:
		return nil, fmt.Errorf("unexpected status code: %v", res.StatusCode)
	}
}
