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

package client

import (
	"context"

	pb "github.com/weaviate/contextionary/contextionary"
)

func (c *Client) version(ctx context.Context) (string, error) {
	m, err := c.grpcClient.Meta(ctx, &pb.MetaParams{})
	if err != nil {
		return "", err
	}

	return m.Version, nil
}

func (c *Client) wordCount(ctx context.Context) (int64, error) {
	m, err := c.grpcClient.Meta(ctx, &pb.MetaParams{})
	if err != nil {
		return 0, err
	}

	return m.WordCount, nil
}

func (c *Client) MetaInfo() (map[string]interface{}, error) {
	c11yVersion, err := c.version(context.Background())
	if err != nil {
		return nil, err
	}

	c11yWordCount, err := c.wordCount(context.Background())
	if err != nil {
		return nil, err
	}

	meta := map[string]interface{}{
		"version":   c11yVersion,
		"wordCount": c11yWordCount,
	}

	return meta, nil
}
