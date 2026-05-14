//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

func (c *Client) MetaInfo() (map[string]any, error) {
	return map[string]any{
		"name":              "DigitalOcean Serverless Inference Module",
		"documentationHref": "https://docs.digitalocean.com/reference/api/reference/serverless-inference/",
	}, nil
}
