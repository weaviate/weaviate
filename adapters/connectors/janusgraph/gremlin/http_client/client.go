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
	"context"
	"fmt"
	"net/http"

	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/sirupsen/logrus"

	"io/ioutil"
)

type Client struct {
	endpoint string
	client   http.Client
	logger   logrus.FieldLogger
}

func NewClient(endpoint string) *Client {
	logger := logrus.New()
	logger.Out = ioutil.Discard

	c := Client{
		endpoint: endpoint,
		client:   http.Client{},
		logger:   logger,
	}

	return &c
}

func (c *Client) SetLogger(logger logrus.FieldLogger) {
	c.logger = logger
}

func (c *Client) Ping() error {
	q := gremlin.RawQuery("1+41")
	response, err := c.Execute(context.Background(), q)
	if err != nil {
		return err
	}

	i, err := response.OneInt()
	if err != nil {
		return err
	}

	if i != 42 {
		return fmt.Errorf("Could not connnected to Gremlin server. Expected the answer to a test query to be 42', but it was %v", i)
	}

	return nil
}
